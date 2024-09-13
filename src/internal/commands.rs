use std::fmt::Display;
#[derive(Debug)]
#[repr(u32)]
pub enum Commands {
    QUIT = 0,
    SUBSCRIBE = 1,
    PUBLISH = 2,
    PING = 3,
    STATS = 4,
    UNKNOWN(String),
}

impl Display for Commands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let command_str = match self {
            Commands::UNKNOWN(s) => s.clone(),
            _ => format!("{:?}", self).to_lowercase(),
        };
        write!(f, "{}", command_str)
    }
}
impl Commands {
    pub fn new() -> Option<Commands> {
        None
    }

    pub fn from_u32(value: u32) -> Commands {
        match value {
            0 => Commands::QUIT,
            1 => Commands::SUBSCRIBE,
            2 => Commands::PUBLISH,
            3 => Commands::PING,
            4 => Commands::STATS,
            _ => Commands::UNKNOWN(format!("Unknown command: {}", value)),
        }
    }
}
