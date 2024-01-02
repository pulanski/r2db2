use std::borrow::Cow;

use owo_colors::OwoColorize;
use reedline::{
    Prompt, PromptEditMode, PromptHistorySearch, PromptHistorySearchStatus, PromptViMode,
};
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct SqlPrompt {
    prefix: String,
    prompt_edit_mode: PromptEditMode,
    prompt_vi_mode: PromptViMode,
    line_count: usize,
}

impl Default for SqlPrompt {
    fn default() -> Self {
        SqlPrompt::builder()
            // .prefix("sql> ".into())
            .prefix(format!("{}{} ", "sql".green().italic(), ">".black()).into())
            .prompt_edit_mode(PromptEditMode::Default)
            .prompt_vi_mode(PromptViMode::Normal)
            .line_count(1)
            .build()
    }
}

impl SqlPrompt {
    pub fn new(prefix: String) -> Self {
        SqlPrompt::builder()
            .prefix(prefix)
            .prompt_edit_mode(PromptEditMode::Default)
            .prompt_vi_mode(PromptViMode::Normal)
            .line_count(1)
            .build()
    }

    pub fn with_edit_mode(mut self, prompt_edit_mode: PromptEditMode) -> Self {
        self.prompt_edit_mode = prompt_edit_mode;
        self
    }

    pub fn with_vi_mode(mut self, prompt_vi_mode: PromptViMode) -> Self {
        self.prompt_vi_mode = prompt_vi_mode;
        self
    }

    pub fn increment_line_count(&mut self) {
        self.line_count += 1;
    }
}

impl Prompt for SqlPrompt {
    fn render_prompt_left(&self) -> Cow<str> {
        let mut prompt = String::new();
        prompt.push_str(&format!(
            "{}{}{} ",
            "[".black(),
            self.line_count.cyan(),
            "]".black()
        ));
        prompt.push_str(&self.prefix);
        prompt.into()
    }

    fn render_prompt_right(&self) -> Cow<str> {
        return "".into();
    }

    fn render_prompt_indicator(&self, prompt_mode: PromptEditMode) -> Cow<str> {
        match prompt_mode {
            PromptEditMode::Default | PromptEditMode::Emacs => "".into(),
            PromptEditMode::Vi(vi_mode) => match vi_mode {
                PromptViMode::Normal => format!("{}", "NORMAL".red()).into(),
                PromptViMode::Insert => format!("{}", "INSERT".green()).into(),
            },
            PromptEditMode::Custom(str) => format!("({str})").into(),
        }
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<str> {
        "... ".black().to_string().into()
    }

    fn render_prompt_history_search_indicator(
        &self,
        history_search: PromptHistorySearch,
    ) -> Cow<str> {
        let prefix = match history_search.status {
            PromptHistorySearchStatus::Passing => "",
            PromptHistorySearchStatus::Failing => "failing ",
        };

        Cow::Owned(format!(
            "({}reverse-search: {}) ",
            prefix, history_search.term
        ))
    }
}
