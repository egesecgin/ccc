use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
    process::Command,
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers,
    },
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap},
    Frame, Terminal,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

type AppResult<T> = Result<T, Box<dyn Error>>;

const APP_NAME: &str = "ccc";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Blocked,
    Failed,
}

impl JobStatus {
    fn label(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "done",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
        }
    }

    fn color(&self) -> Color {
        match self {
            Self::Queued => Color::Cyan,
            Self::Running => Color::Yellow,
            Self::Succeeded => Color::Green,
            Self::Blocked => Color::Magenta,
            Self::Failed => Color::Red,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    id: Uuid,
    session_id: String,
    working_dir: PathBuf,
    prompt: String,
    scheduled_at: i64,
    model: String,
    effort: String,
    status: JobStatus,
    attempts: u32,
    last_message: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct QueueState {
    jobs: Vec<Job>,
}

#[derive(Debug, Clone)]
struct SessionCandidate {
    session_id: String,
    working_dir: PathBuf,
    modified: SystemTime,
}

struct RunResult {
    job_id: Uuid,
    status: JobStatus,
    message: String,
}

struct Composer {
    session_id: String,
    working_dir: String,
    prompt: String,
    scheduled_for: String,
    model: String,
    effort: String,
    active_field: usize,
    session_cursor: usize,
    error: Option<String>,
}

impl Composer {
    fn new(session: Option<&SessionCandidate>) -> Self {
        let mut form = Self {
            session_id: String::new(),
            working_dir: String::new(),
            prompt: "Continue where you left off.".to_owned(),
            scheduled_for: Local::now().format("%Y-%m-%d %H:%M").to_string(),
            model: "opus".to_owned(),
            effort: "high".to_owned(),
            active_field: 0,
            session_cursor: 0,
            error: None,
        };
        if let Some(session) = session {
            form.apply_session(session);
        }
        form
    }

    fn apply_session(&mut self, session: &SessionCandidate) {
        self.session_id = session.session_id.clone();
        if !session.working_dir.as_os_str().is_empty() {
            self.working_dir = session.working_dir.display().to_string();
        }
    }

    fn current_value_mut(&mut self) -> &mut String {
        match self.active_field {
            0 => &mut self.session_id,
            1 => &mut self.working_dir,
            2 => &mut self.prompt,
            3 => &mut self.scheduled_for,
            4 => &mut self.model,
            _ => &mut self.effort,
        }
    }

    fn value(&self, index: usize) -> &str {
        match index {
            0 => &self.session_id,
            1 => &self.working_dir,
            2 => &self.prompt,
            3 => &self.scheduled_for,
            4 => &self.model,
            _ => &self.effort,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FocusedPanel {
    Jobs,
    Sessions,
}

impl FocusedPanel {
    fn toggle(self) -> Self {
        match self {
            Self::Jobs => Self::Sessions,
            Self::Sessions => Self::Jobs,
        }
    }
}

struct App {
    state_path: PathBuf,
    state: QueueState,
    sessions: Vec<SessionCandidate>,
    job_selected: usize,
    session_selected: usize,
    focused_panel: FocusedPanel,
    pending_cancel: Option<Uuid>,
    composer: Option<Composer>,
    notice: String,
    active_job: Option<Uuid>,
    result_tx: Sender<RunResult>,
    result_rx: Receiver<RunResult>,
    quit: bool,
}

impl App {
    fn load(state_path: PathBuf) -> AppResult<Self> {
        let mut state = load_state(&state_path)?;
        recover_interrupted_jobs(&mut state);
        save_state(&state_path, &state)?;
        let (result_tx, result_rx) = mpsc::channel();
        Ok(Self {
            state_path,
            state,
            sessions: discover_sessions(),
            job_selected: 0,
            session_selected: 0,
            focused_panel: FocusedPanel::Sessions,
            pending_cancel: None,
            composer: None,
            notice: "Sessions are read-only. Select one and press Enter or n to queue a prompt."
                .to_owned(),
            active_job: None,
            result_tx,
            result_rx,
            quit: false,
        })
    }

    fn save(&mut self) {
        if let Err(error) = save_state(&self.state_path, &self.state) {
            self.notice = format!("Could not save queue: {error}");
        }
    }

    fn refresh_sessions(&mut self) {
        self.sessions = discover_sessions();
        self.session_selected = self
            .session_selected
            .min(self.sessions.len().saturating_sub(1));
        self.notice = format!("Found {} Claude session(s).", self.sessions.len());
    }

    fn tick(&mut self) {
        while let Ok(result) = self.result_rx.try_recv() {
            if let Some(job) = self
                .state
                .jobs
                .iter_mut()
                .find(|job| job.id == result.job_id)
            {
                job.status = result.status.clone();
                job.last_message = result.message.clone();
                self.notice = format!("{}: {}", result.status.label(), result.message);
            }
            self.active_job = None;
            self.save();
        }

        if self.composer.is_none() && self.active_job.is_none() {
            self.start_next_due_job();
        }
    }

    fn start_next_due_job(&mut self) {
        let now = Local::now().timestamp();
        let Some(index) = self
            .state
            .jobs
            .iter()
            .position(|job| job.status == JobStatus::Queued && job.scheduled_at <= now)
        else {
            return;
        };

        let job = {
            let job = &mut self.state.jobs[index];
            job.status = JobStatus::Running;
            job.attempts += 1;
            job.last_message = "Starting Claude Code...".to_owned();
            job.clone()
        };
        self.active_job = Some(job.id);
        self.notice = format!("Running {}", shorten(&job.session_id, 12));
        self.save();

        let tx = self.result_tx.clone();
        thread::spawn(move || {
            let _ = tx.send(run_job(&job));
        });
    }

    fn move_selection(&mut self, delta: isize) {
        let (len, selected) = match self.focused_panel {
            FocusedPanel::Jobs => (self.state.jobs.len(), &mut self.job_selected),
            FocusedPanel::Sessions => (self.sessions.len(), &mut self.session_selected),
        };
        if len == 0 {
            *selected = 0;
            return;
        }
        *selected = (*selected as isize + delta).clamp(0, len as isize - 1) as usize;
    }

    fn open_composer(&mut self) {
        let session = self.sessions.get(self.session_selected);
        let mut composer = Composer::new(session);
        composer.session_cursor = self.session_selected;
        self.composer = Some(composer);
    }

    fn choose_next_session(&mut self) {
        if self.sessions.is_empty() {
            if let Some(form) = self.composer.as_mut() {
                form.error = Some("No Claude sessions found in ~/.claude/projects.".to_owned());
            }
            return;
        }
        let session = {
            let form = self.composer.as_mut().expect("composer exists");
            form.session_cursor = (form.session_cursor + 1) % self.sessions.len();
            self.sessions[form.session_cursor].clone()
        };
        if let Some(form) = self.composer.as_mut() {
            form.apply_session(&session);
            form.error = None;
        }
    }

    fn submit_composer(&mut self) {
        let mut form = self.composer.take().expect("composer exists");
        let scheduled_at = match parse_schedule(&form.scheduled_for) {
            Ok(time) => time,
            Err(error) => {
                form.error = Some(error);
                self.composer = Some(form);
                return;
            }
        };
        if form.session_id.trim().is_empty()
            || form.working_dir.trim().is_empty()
            || form.prompt.trim().is_empty()
        {
            form.error = Some("Session ID, working directory, and prompt are required.".to_owned());
            self.composer = Some(form);
            return;
        }

        let id = Uuid::new_v4();
        self.state.jobs.push(Job {
            id,
            session_id: form.session_id.trim().to_owned(),
            working_dir: PathBuf::from(form.working_dir.trim()),
            prompt: form.prompt.trim().to_owned(),
            scheduled_at,
            model: form.model.trim().to_owned(),
            effort: form.effort.trim().to_owned(),
            status: JobStatus::Queued,
            attempts: 0,
            last_message: "Awaiting scheduled time.".to_owned(),
        });
        self.state.jobs.sort_by_key(|job| job.scheduled_at);
        self.job_selected = self
            .state
            .jobs
            .iter()
            .position(|job| job.id == id)
            .unwrap_or_default();
        self.notice = "Continuation queued.".to_owned();
        self.save();
    }

    fn run_selected_now(&mut self) {
        if self.focused_panel != FocusedPanel::Jobs {
            self.notice =
                "Select a scheduled job first. Discovered sessions are read-only.".to_owned();
            return;
        }
        if self.active_job.is_some() {
            self.notice = "A Claude job is already running.".to_owned();
            return;
        }
        if let Some(job) = self.state.jobs.get_mut(self.job_selected) {
            if job.status == JobStatus::Running {
                self.notice = "That job is already running.".to_owned();
                return;
            }
            job.status = JobStatus::Queued;
            job.scheduled_at = Local::now().timestamp();
            job.last_message = "Run requested now.".to_owned();
            self.save();
        }
    }

    fn request_cancel_selected_job(&mut self) {
        if self.focused_panel != FocusedPanel::Jobs {
            self.notice =
                "Sessions cannot be deleted. They are read-only Claude history files.".to_owned();
            return;
        }
        let Some(job) = self.state.jobs.get(self.job_selected) else {
            return;
        };
        if job.status == JobStatus::Running {
            self.notice = "Running jobs cannot be cancelled.".to_owned();
            return;
        }
        if self.pending_cancel != Some(job.id) {
            self.pending_cancel = Some(job.id);
            self.notice =
                "Press x again to cancel this queued job only. Claude sessions are never deleted."
                    .to_owned();
            return;
        }
        self.state.jobs.remove(self.job_selected);
        self.job_selected = self
            .job_selected
            .min(self.state.jobs.len().saturating_sub(1));
        self.pending_cancel = None;
        self.notice = "Queued job cancelled. The Claude session was not changed.".to_owned();
        self.save();
    }

    fn handle_key(&mut self, key: KeyEvent) {
        if self.composer.is_some() {
            self.handle_composer_key(key);
            return;
        }
        if self.pending_cancel.is_some() && key.code != KeyCode::Char('x') {
            self.pending_cancel = None;
            self.notice = "Cancellation not confirmed. No changes made.".to_owned();
        }

        match key.code {
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Char('n') => self.open_composer(),
            KeyCode::Enter if self.focused_panel == FocusedPanel::Sessions => self.open_composer(),
            KeyCode::Char('r') => self.run_selected_now(),
            KeyCode::Char('x') => self.request_cancel_selected_job(),
            KeyCode::Tab => self.focused_panel = self.focused_panel.toggle(),
            KeyCode::Char('j') | KeyCode::Down => self.move_selection(1),
            KeyCode::Char('k') | KeyCode::Up => self.move_selection(-1),
            KeyCode::F(5) => self.refresh_sessions(),
            _ => {}
        }
    }

    fn handle_composer_key(&mut self, key: KeyEvent) {
        if key.code == KeyCode::Esc {
            self.composer = None;
            self.notice = "Queue form cancelled.".to_owned();
            return;
        }
        if key.code == KeyCode::Enter && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.submit_composer();
            return;
        }
        if key.code == KeyCode::F(2) {
            self.choose_next_session();
            return;
        }

        let form = self.composer.as_mut().expect("composer exists");
        form.error = None;
        match key.code {
            KeyCode::Tab => form.active_field = (form.active_field + 1) % 6,
            KeyCode::BackTab => form.active_field = (form.active_field + 5) % 6,
            KeyCode::Enter if form.active_field == 2 => form.current_value_mut().push('\n'),
            KeyCode::Enter => form.active_field = (form.active_field + 1) % 6,
            KeyCode::Backspace => {
                form.current_value_mut().pop();
            }
            KeyCode::Delete => {
                form.current_value_mut().clear();
            }
            KeyCode::Char(character) => form.current_value_mut().push(character),
            _ => {}
        }
    }
}

fn main() -> AppResult<()> {
    match env::args().nth(1).as_deref() {
        Some("--help" | "-h") => {
            print_usage();
            Ok(())
        }
        Some("--version" | "-V") => {
            println!("ccc {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("--worker") => run_worker(queue_state_path()?),
        None => run_tui(queue_state_path()?),
        Some(argument) => {
            eprintln!("Unknown option: {argument}");
            print_usage();
            Ok(())
        }
    }
}

fn print_usage() {
    println!(
        "Continue Claude Code\n\n\
         Usage:\n  ccc            Open the queue\n  ccc --worker   Run scheduled jobs without the interface\n\
         ccc finds local Claude sessions, queues a prompt, and continues the selected session at the chosen time."
    );
}

fn run_tui(state_path: PathBuf) -> AppResult<()> {
    let mut app = App::load(state_path)?;
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_event_loop(&mut terminal, &mut app);

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    result
}

fn run_event_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
) -> AppResult<()> {
    while !app.quit {
        terminal.draw(|frame| draw_ui(frame, app))?;
        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                app.handle_key(key);
            }
        }
        app.tick();
    }
    Ok(())
}

fn run_worker(state_path: PathBuf) -> AppResult<()> {
    println!("ccc worker started. Press Ctrl+C to stop.");
    loop {
        let mut state = load_state(&state_path)?;
        recover_interrupted_jobs(&mut state);
        let now = Local::now().timestamp();
        if let Some(index) = state
            .jobs
            .iter()
            .position(|job| job.status == JobStatus::Queued && job.scheduled_at <= now)
        {
            let job = {
                let job = &mut state.jobs[index];
                job.status = JobStatus::Running;
                job.attempts += 1;
                job.last_message = "Starting Claude Code...".to_owned();
                job.clone()
            };
            save_state(&state_path, &state)?;
            println!("Running job for session {}", job.session_id);
            let result = run_job(&job);
            if let Some(job) = state.jobs.iter_mut().find(|job| job.id == result.job_id) {
                job.status = result.status.clone();
                job.last_message = result.message.clone();
            }
            save_state(&state_path, &state)?;
            println!("{}: {}", result.status.label(), result.message);
            continue;
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn run_job(job: &Job) -> RunResult {
    if !job.working_dir.is_dir() {
        return RunResult {
            job_id: job.id,
            status: JobStatus::Failed,
            message: format!(
                "Working directory does not exist: {}",
                job.working_dir.display()
            ),
        };
    }

    let output = Command::new("claude")
        .current_dir(&job.working_dir)
        .arg("--resume")
        .arg(&job.session_id)
        .arg("--model")
        .arg(&job.model)
        .arg("--effort")
        .arg(&job.effort)
        .arg("--print")
        .arg(&job.prompt)
        .output();

    match output {
        Ok(output) => {
            let combined = format!(
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let message = summarize_output(&combined);
            let lower = combined.to_lowercase();
            let status = if lower.contains("usage limit")
                || lower.contains("rate limit")
                || lower.contains("too many requests")
            {
                JobStatus::Blocked
            } else if output.status.success() {
                JobStatus::Succeeded
            } else {
                JobStatus::Failed
            };
            RunResult {
                job_id: job.id,
                status,
                message,
            }
        }
        Err(error) => RunResult {
            job_id: job.id,
            status: JobStatus::Failed,
            message: format!("Could not start Claude Code: {error}"),
        },
    }
}

fn summarize_output(output: &str) -> String {
    let lines: Vec<&str> = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect();
    if lines.is_empty() {
        "Claude Code exited without output.".to_owned()
    } else {
        shorten(&lines[lines.len() - 1..].join(" "), 240)
    }
}

fn queue_state_path() -> AppResult<PathBuf> {
    let base = dirs::data_local_dir()
        .or_else(dirs::data_dir)
        .ok_or("Could not determine a local application-data directory.")?;
    let directory = base.join(APP_NAME);
    fs::create_dir_all(&directory)?;
    Ok(directory.join("queue.json"))
}

fn load_state(path: &Path) -> AppResult<QueueState> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(serde_json::from_str(&contents)?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(QueueState::default()),
        Err(error) => Err(Box::new(error)),
    }
}

fn save_state(path: &Path, state: &QueueState) -> AppResult<()> {
    let temporary = path.with_extension("json.tmp");
    let contents = serde_json::to_string_pretty(state)?;
    fs::write(&temporary, contents)?;
    if path.exists() {
        fs::remove_file(path)?;
    }
    fs::rename(temporary, path)?;
    Ok(())
}

fn recover_interrupted_jobs(state: &mut QueueState) {
    for job in &mut state.jobs {
        if job.status == JobStatus::Running {
            job.status = JobStatus::Queued;
            job.last_message = "Recovered after ccc stopped while the job was running.".to_owned();
        }
    }
}

fn parse_schedule(input: &str) -> Result<i64, String> {
    let input = input.trim();
    if input.eq_ignore_ascii_case("now") {
        return Ok(Local::now().timestamp());
    }
    let naive = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M")
        .map_err(|_| "Use `now` or a local time such as 2026-07-16 20:30.".to_owned())?;
    Local
        .from_local_datetime(&naive)
        .single()
        .map(|time| time.timestamp())
        .ok_or_else(|| {
            "That local time is ambiguous or does not exist because of daylight saving time."
                .to_owned()
        })
}

fn format_schedule(timestamp: i64) -> String {
    Local
        .timestamp_opt(timestamp, 0)
        .single()
        .map(|time| time.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "unknown time".to_owned())
}

fn discover_sessions() -> Vec<SessionCandidate> {
    let mut sessions = Vec::new();
    for root in claude_projects_roots() {
        let Ok(projects) = fs::read_dir(root) else {
            continue;
        };
        for project in projects.flatten() {
            let path = project.path();
            if !path.is_dir() {
                continue;
            };
            let Ok(files) = fs::read_dir(path) else {
                continue;
            };
            for file in files.flatten() {
                let path = file.path();
                if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
                    continue;
                }
                let Some(session_id) = path.file_stem().and_then(|stem| stem.to_str()) else {
                    continue;
                };
                let modified = file
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                sessions.push(SessionCandidate {
                    session_id: session_id.to_owned(),
                    working_dir: read_session_working_dir(&path).unwrap_or_default(),
                    modified,
                });
            }
        }
    }
    sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    sessions.dedup_by(|left, right| left.session_id == right.session_id);
    sessions.sort_by_key(|session| std::cmp::Reverse(session.modified));
    sessions
}

fn claude_projects_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(config_dir) = env::var_os("CLAUDE_CONFIG_DIR").map(PathBuf::from) {
        roots.push(projects_directory(config_dir));
    }
    if let Some(home) = dirs::home_dir() {
        roots.push(home.join(".claude/projects"));
    }
    if let Ok(current_dir) = env::current_dir() {
        for directory in current_dir.ancestors() {
            roots.push(directory.join(".claude/projects"));
        }
    }
    roots.sort();
    roots.dedup();
    roots
}

fn projects_directory(config_dir: PathBuf) -> PathBuf {
    if config_dir.file_name().and_then(|name| name.to_str()) == Some("projects") {
        config_dir
    } else {
        config_dir.join("projects")
    }
}

fn read_session_working_dir(path: &Path) -> Option<PathBuf> {
    let file = File::open(path).ok()?;
    for line in BufReader::new(file).lines().take(24).flatten() {
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if let Some(cwd) = find_cwd(&value) {
            return Some(PathBuf::from(cwd));
        }
    }
    None
}

fn find_cwd(value: &Value) -> Option<&str> {
    match value {
        Value::Object(object) => {
            if let Some(cwd) = object.get("cwd").and_then(Value::as_str) {
                return Some(cwd);
            }
            object.values().find_map(find_cwd)
        }
        Value::Array(values) => values.iter().find_map(find_cwd),
        _ => None,
    }
}

fn shorten(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let shortened: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{shortened}...")
    } else {
        shortened
    }
}

fn draw_ui(frame: &mut Frame, app: &App) {
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(2),
        Constraint::Percentage(36),
        Constraint::Min(9),
        Constraint::Length(3),
    ])
    .margin(1)
    .split(frame.area());

    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            "ccc",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  Claude Code continuation queue"),
        Span::styled(
            format!("   {} discovered session(s)", app.sessions.len()),
            Style::default().fg(Color::LightCyan),
        ),
    ]))
    .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header, chunks[0]);

    let guidance = Paragraph::new(
        "Discovered sessions are read-only. Select one below and press Enter or n to queue a prompt.",
    )
    .style(Style::default().fg(Color::Gray));
    frame.render_widget(guidance, chunks[1]);

    draw_jobs_panel(frame, app, chunks[2]);
    draw_sessions_panel(frame, app, chunks[3]);

    let footer = Paragraph::new(Text::from(vec![
        Line::from(
            "Tab switch pane   j/k or arrows scroll   n or Enter queue session   r run job now   x cancel queued job   F5 refresh   q quit",
        ),
        Line::styled(&app.notice, Style::default().fg(Color::Gray)),
    ]))
    .wrap(Wrap { trim: true })
    .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, chunks[4]);

    if let Some(form) = &app.composer {
        draw_composer(frame, form, &app.sessions);
    }
}

fn draw_jobs_panel(frame: &mut Frame, app: &App, area: Rect) {
    let header_cells = [
        "Scheduled",
        "Status",
        "Session",
        "Model",
        "Prompt",
        "Last result",
    ]
    .into_iter()
    .map(|title| Cell::from(title).style(Style::default().add_modifier(Modifier::BOLD)));
    let (start, end) = visible_slice(
        app.state.jobs.len(),
        app.job_selected,
        usize::from(area.height.saturating_sub(4)),
    );
    let rows = app.state.jobs[start..end]
        .iter()
        .enumerate()
        .map(|(offset, job)| {
            let index = start + offset;
            let selection = if index == app.job_selected && app.focused_panel == FocusedPanel::Jobs
            {
                Style::default()
                    .bg(Color::Cyan)
                    .fg(Color::Black)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(format_schedule(job.scheduled_at)),
                Cell::from(job.status.label()).style(Style::default().fg(job.status.color())),
                Cell::from(shorten(&job.session_id, 12)),
                Cell::from(format!("{}/{}", job.model, job.effort)),
                Cell::from(shorten(&job.prompt.replace('\n', " "), 36)),
                Cell::from(shorten(&job.last_message, 42)),
            ])
            .style(selection)
        });
    let title = format!(
        " Scheduled jobs ({}/{}) ",
        displayed_position(app.job_selected, app.state.jobs.len()),
        app.state.jobs.len()
    );
    let table = Table::new(
        rows,
        [
            Constraint::Length(17),
            Constraint::Length(9),
            Constraint::Length(14),
            Constraint::Length(16),
            Constraint::Percentage(27),
            Constraint::Percentage(27),
        ],
    )
    .header(Row::new(header_cells).bottom_margin(1))
    .block(panel_block(title, app.focused_panel == FocusedPanel::Jobs))
    .column_spacing(1);
    frame.render_widget(table, area);
}

fn draw_sessions_panel(frame: &mut Frame, app: &App, area: Rect) {
    let (start, end) = visible_slice(
        app.sessions.len(),
        app.session_selected,
        usize::from(area.height.saturating_sub(4)),
    );
    let rows = app.sessions[start..end]
        .iter()
        .enumerate()
        .map(|(offset, session)| {
            let index = start + offset;
            let style =
                if index == app.session_selected && app.focused_panel == FocusedPanel::Sessions {
                    Style::default()
                        .bg(Color::Cyan)
                        .fg(Color::Black)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
            let project = if session.working_dir.as_os_str().is_empty() {
                "Project folder unavailable".to_owned()
            } else {
                session.working_dir.display().to_string()
            };
            Row::new(vec![
                Cell::from(format_session_modified(session.modified)),
                Cell::from(shorten(&session.session_id, 20)),
                Cell::from(shorten(&project, 72)),
            ])
            .style(style)
        });
    let header = ["Modified", "Session", "Project folder"]
        .into_iter()
        .map(|title| Cell::from(title).style(Style::default().add_modifier(Modifier::BOLD)));
    let title = format!(
        " Discovered sessions - read-only ({}/{}) ",
        displayed_position(app.session_selected, app.sessions.len()),
        app.sessions.len()
    );
    let table = Table::new(
        rows,
        [
            Constraint::Length(17),
            Constraint::Length(23),
            Constraint::Min(24),
        ],
    )
    .header(Row::new(header).bottom_margin(1))
    .block(panel_block(
        title,
        app.focused_panel == FocusedPanel::Sessions,
    ))
    .column_spacing(1);
    frame.render_widget(table, area);
}

fn panel_block(title: String, focused: bool) -> Block<'static> {
    let border_color = if focused { Color::Cyan } else { Color::Gray };
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
}

fn visible_slice(total: usize, selected: usize, capacity: usize) -> (usize, usize) {
    if total == 0 {
        return (0, 0);
    }
    let capacity = capacity.max(1).min(total);
    let selected = selected.min(total - 1);
    let start = selected
        .saturating_sub(capacity / 2)
        .min(total.saturating_sub(capacity));
    (start, start + capacity)
}

fn displayed_position(selected: usize, total: usize) -> usize {
    if total == 0 {
        0
    } else {
        selected.min(total - 1) + 1
    }
}

fn format_session_modified(time: SystemTime) -> String {
    DateTime::<Local>::from(time)
        .format("%Y-%m-%d %H:%M")
        .to_string()
}

fn draw_composer(frame: &mut Frame, form: &Composer, sessions: &[SessionCandidate]) {
    let area = centered_rect(84, 78, frame.area());
    frame.render_widget(Clear, area);
    let fields = [
        ("Session ID", form.value(0)),
        ("Working directory", form.value(1)),
        ("Prompt", form.value(2)),
        ("Run at", form.value(3)),
        ("Model", form.value(4)),
        ("Effort", form.value(5)),
    ];
    let mut lines = vec![Line::styled(
        "Queue a Claude Code continuation",
        Style::default().add_modifier(Modifier::BOLD),
    )];
    for (index, (label, value)) in fields.iter().enumerate() {
        let style = if index == form.active_field {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        let display = if *label == "Prompt" {
            value.replace('\n', " ")
        } else {
            (*value).to_owned()
        };
        lines.push(Line::from(vec![
            Span::styled(format!("{:>18}: ", label), style),
            Span::styled(shorten(&display, 100), style),
        ]));
    }
    lines.push(Line::raw(""));
    lines.push(Line::styled(
        format!(
            "F2 chooses next session. {} session(s) discovered. Tab changes field.",
            sessions.len()
        ),
        Style::default().fg(Color::DarkGray),
    ));
    lines.push(Line::styled(
        "Enter moves on (or adds a prompt line). Ctrl+Enter queues. Esc cancels.",
        Style::default().fg(Color::DarkGray),
    ));
    if let Some(error) = &form.error {
        lines.push(Line::styled(error, Style::default().fg(Color::Red)));
    }

    let dialog = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .block(Block::default().title(" New job ").borders(Borders::ALL))
        .alignment(Alignment::Left);
    frame.render_widget(dialog, area);
}

fn centered_rect(width_percent: u16, height_percent: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - height_percent) / 2),
        Constraint::Percentage(height_percent),
        Constraint::Percentage((100 - height_percent) / 2),
    ])
    .split(area);
    Layout::horizontal([
        Constraint::Percentage((100 - width_percent) / 2),
        Constraint::Percentage(width_percent),
        Constraint::Percentage((100 - width_percent) / 2),
    ])
    .split(vertical[1])[1]
}

#[cfg(test)]
mod tests {
    use super::{parse_schedule, shorten, visible_slice};

    #[test]
    fn parses_now() {
        assert!(parse_schedule("now").is_ok());
    }

    #[test]
    fn rejects_invalid_schedule() {
        assert!(parse_schedule("tomorrow afternoon").is_err());
    }

    #[test]
    fn shortens_long_text() {
        assert_eq!(shorten("abcdef", 3), "abc...");
        assert_eq!(shorten("abc", 3), "abc");
    }

    #[test]
    fn visible_slice_keeps_the_selection_in_view() {
        assert_eq!(visible_slice(64, 0, 5), (0, 5));
        assert_eq!(visible_slice(64, 32, 5), (30, 35));
        assert_eq!(visible_slice(64, 63, 5), (59, 64));
    }
}
