use std::fmt::Display;

#[derive(Debug)]
#[repr(u8)]
pub enum Commands {
    SUBSCRIBE = 1,
    SEND = 2,
    // TOPIC = 3,
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

    pub fn from_u8(value: u8) -> Commands {
        match value {
            1 => Commands::SUBSCRIBE,
            2 => Commands::SEND,
            // 3 => Commands::TOPIC,
            // 4 => Commands::MESSAGE,
            _ => Commands::UNKNOWN(format!("Unknown command: {}", value)),
        }
    }
}
