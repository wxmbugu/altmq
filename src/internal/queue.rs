use crate::Message;

pub struct Queue<'a> {
    pub name: String,
    pub message: Message<'a>,
}
