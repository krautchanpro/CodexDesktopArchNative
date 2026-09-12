use adw::prelude::*;
use gtk::{glib::translate::IntoGlib, pango};
use pulldown_cmark::{Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use sourceview::prelude::*;

/// Render Markdown as native GTK widgets. Prose uses TextBuffer/Pango tags;
/// fenced code blocks use GtkSourceView for selection, syntax highlighting,
/// line numbers, and accessible keyboard navigation.
pub fn render_rich(markdown: &str) -> gtk::Box {
    let root = gtk::Box::new(gtk::Orientation::Vertical, 8);
    root.add_css_class("markdown-view");

    let mut prose = String::new();
    let mut code = String::new();
    let mut language = String::new();
    let mut in_fence = false;

    for line in markdown.lines() {
        if let Some(marker) = line.trim_start().strip_prefix("```") {
            if in_fence {
                append_code_block(&root, language.trim(), code.trim_end_matches('\n'));
                code.clear();
                language.clear();
                in_fence = false;
            } else {
                append_prose(&root, &prose);
                prose.clear();
                language.push_str(marker.trim());
                in_fence = true;
            }
            continue;
        }

        if in_fence {
            code.push_str(line);
            code.push('\n');
        } else {
            prose.push_str(line);
            prose.push('\n');
        }
    }

    if in_fence {
        // An unfinished fence is still useful while a response streams.
        append_code_block(&root, language.trim(), code.trim_end_matches('\n'));
    } else {
        append_prose(&root, &prose);
    }
    root
}

fn append_prose(root: &gtk::Box, prose: &str) {
    if !prose.trim().is_empty() {
        root.append(&render(prose.trim_end()));
    }
}

fn append_code_block(root: &gtk::Box, language_id: &str, code: &str) {
    let shell = gtk::Box::new(gtk::Orientation::Vertical, 0);
    shell.add_css_class("code-block-shell");

    let toolbar = gtk::Box::new(gtk::Orientation::Horizontal, 6);
    toolbar.add_css_class("code-block-toolbar");
    let language_label = gtk::Label::new(Some(if language_id.is_empty() {
        "code"
    } else {
        language_id
    }));
    language_label.set_xalign(0.0);
    language_label.set_hexpand(true);
    language_label.add_css_class("caption");
    toolbar.append(&language_label);
    let copy = gtk::Button::builder()
        .icon_name("edit-copy-symbolic")
        .tooltip_text("Copy code")
        .has_frame(false)
        .build();
    let copied = code.to_owned();
    copy.connect_clicked(move |_| {
        if let Some(display) = gtk::gdk::Display::default() {
            display.clipboard().set_text(&copied);
        }
    });
    toolbar.append(&copy);
    shell.append(&toolbar);

    let buffer = sourceview::Buffer::new(None::<&gtk::TextTagTable>);
    buffer.set_text(code);
    configure_source_buffer(&buffer, language_id);
    let view = sourceview::View::with_buffer(&buffer);
    view.set_editable(false);
    view.set_cursor_visible(false);
    view.set_monospace(true);
    view.set_show_line_numbers(true);
    view.set_highlight_current_line(false);
    view.set_wrap_mode(gtk::WrapMode::None);
    view.set_top_margin(8);
    view.set_bottom_margin(8);
    view.set_left_margin(8);
    view.set_right_margin(8);
    view.add_css_class("source-code-view");

    let line_count = code.lines().count().clamp(2, 20) as i32;
    let scroller = gtk::ScrolledWindow::builder()
        .hscrollbar_policy(gtk::PolicyType::Automatic)
        .vscrollbar_policy(gtk::PolicyType::Automatic)
        .min_content_height(line_count * 22)
        .max_content_height(line_count * 22)
        .propagate_natural_height(true)
        .child(&view)
        .build();
    shell.append(&scroller);
    root.append(&shell);
}

/// Render CommonMark/GFM into a native GTK text widget. Raw HTML is displayed
/// as source text; it is never interpreted, so this module cannot instantiate a
/// browser or execute script.
pub fn render(markdown: &str) -> gtk::TextView {
    let buffer = gtk::TextBuffer::new(None::<&gtk::TextTagTable>);
    install_tags(&buffer);

    let mut active_tags: Vec<&'static str> = Vec::new();
    let mut list_stack: Vec<ListState> = Vec::new();
    let mut link_stack: Vec<String> = Vec::new();
    let mut options = Options::empty();
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TASKLISTS);
    options.insert(Options::ENABLE_GFM);
    options.insert(Options::ENABLE_FOOTNOTES);

    for event in Parser::new_ext(markdown, options) {
        let mut cursor = buffer.end_iter();
        match event {
            Event::Start(tag) => match tag {
                Tag::Heading { level, .. } => {
                    ensure_block_break(&buffer, &mut cursor);
                    active_tags.push(heading_tag(level));
                }
                Tag::BlockQuote(_) => active_tags.push("quote"),
                Tag::CodeBlock(_) => {
                    ensure_block_break(&buffer, &mut cursor);
                    active_tags.push("code-block");
                }
                Tag::List(start) => list_stack.push(ListState::new(start)),
                Tag::Item => {
                    ensure_line_start(&buffer, &mut cursor);
                    let prefix = list_stack
                        .last_mut()
                        .map(ListState::next_prefix)
                        .unwrap_or_else(|| "• ".into());
                    insert(&buffer, &mut cursor, &prefix, &["list-marker"]);
                }
                Tag::Emphasis => active_tags.push("emphasis"),
                Tag::Strong => active_tags.push("strong"),
                Tag::Strikethrough => active_tags.push("strikethrough"),
                Tag::Superscript => active_tags.push("superscript"),
                Tag::Subscript => active_tags.push("subscript"),
                Tag::Link { dest_url, .. } => {
                    link_stack.push(dest_url.into_string());
                    active_tags.push("link");
                }
                Tag::Image { dest_url, .. } => {
                    link_stack.push(dest_url.into_string());
                    insert(&buffer, &mut cursor, "Image: ", &["emphasis"]);
                }
                Tag::Table(_) => ensure_block_break(&buffer, &mut cursor),
                Tag::TableHead => active_tags.push("table-head"),
                Tag::TableCell => {
                    if !at_line_start(&buffer) {
                        insert(&buffer, &mut cursor, "  │  ", &["table-rule"]);
                    }
                }
                Tag::FootnoteDefinition(label) => {
                    ensure_block_break(&buffer, &mut cursor);
                    insert(&buffer, &mut cursor, &format!("[{label}] "), &["link"]);
                }
                Tag::HtmlBlock | Tag::MetadataBlock(_) => active_tags.push("code-inline"),
                Tag::DefinitionListTitle => active_tags.push("strong"),
                Tag::DefinitionListDefinition => active_tags.push("quote"),
                Tag::Paragraph | Tag::TableRow | Tag::DefinitionList => {}
            },
            Event::End(tag) => match tag {
                TagEnd::Paragraph => append_newlines(&buffer, &mut cursor, 2),
                TagEnd::Heading(level) => {
                    remove_last(&mut active_tags, heading_tag(level));
                    append_newlines(&buffer, &mut cursor, 2);
                }
                TagEnd::BlockQuote(_) => {
                    remove_last(&mut active_tags, "quote");
                    append_newlines(&buffer, &mut cursor, 2);
                }
                TagEnd::CodeBlock => {
                    remove_last(&mut active_tags, "code-block");
                    append_newlines(&buffer, &mut cursor, 2);
                }
                TagEnd::List(_) => {
                    list_stack.pop();
                    append_newlines(&buffer, &mut cursor, 1);
                }
                TagEnd::Item => append_newlines(&buffer, &mut cursor, 1),
                TagEnd::Emphasis => remove_last(&mut active_tags, "emphasis"),
                TagEnd::Strong => remove_last(&mut active_tags, "strong"),
                TagEnd::Strikethrough => remove_last(&mut active_tags, "strikethrough"),
                TagEnd::Superscript => remove_last(&mut active_tags, "superscript"),
                TagEnd::Subscript => remove_last(&mut active_tags, "subscript"),
                TagEnd::Link => {
                    remove_last(&mut active_tags, "link");
                    if let Some(url) = link_stack.pop() {
                        insert(&buffer, &mut cursor, &format!("  <{url}>"), &["link-url"]);
                    }
                }
                TagEnd::Image => {
                    if let Some(url) = link_stack.pop() {
                        insert(&buffer, &mut cursor, &format!("  <{url}>"), &["link-url"]);
                    }
                }
                TagEnd::TableRow => append_newlines(&buffer, &mut cursor, 1),
                TagEnd::Table => append_newlines(&buffer, &mut cursor, 2),
                TagEnd::TableHead => remove_last(&mut active_tags, "table-head"),
                TagEnd::HtmlBlock | TagEnd::MetadataBlock(_) => {
                    remove_last(&mut active_tags, "code-inline")
                }
                TagEnd::DefinitionListTitle => remove_last(&mut active_tags, "strong"),
                TagEnd::DefinitionListDefinition => remove_last(&mut active_tags, "quote"),
                TagEnd::FootnoteDefinition | TagEnd::TableCell | TagEnd::DefinitionList => {}
            },
            Event::Text(text) => insert(&buffer, &mut cursor, &text, &active_tags),
            Event::Code(code) => insert(&buffer, &mut cursor, &code, &["code-inline"]),
            Event::InlineMath(math) => {
                insert(&buffer, &mut cursor, &format!("${math}$"), &["code-inline"])
            }
            Event::DisplayMath(math) => {
                ensure_block_break(&buffer, &mut cursor);
                insert(
                    &buffer,
                    &mut cursor,
                    &format!("$${math}$$"),
                    &["code-block"],
                );
                append_newlines(&buffer, &mut cursor, 2);
            }
            Event::Html(html) | Event::InlineHtml(html) => {
                insert(&buffer, &mut cursor, &html, &["code-inline"])
            }
            Event::FootnoteReference(label) => {
                insert(&buffer, &mut cursor, &format!("[{label}]"), &["link"])
            }
            Event::SoftBreak => insert(&buffer, &mut cursor, " ", &active_tags),
            Event::HardBreak => insert(&buffer, &mut cursor, "\n", &active_tags),
            Event::Rule => {
                ensure_block_break(&buffer, &mut cursor);
                insert(
                    &buffer,
                    &mut cursor,
                    "────────────────────────",
                    &["table-rule"],
                );
                append_newlines(&buffer, &mut cursor, 2);
            }
            Event::TaskListMarker(checked) => {
                insert(
                    &buffer,
                    &mut cursor,
                    if checked { "☑ " } else { "☐ " },
                    &["list-marker"],
                );
            }
        }
    }

    let view = gtk::TextView::with_buffer(&buffer);
    view.set_editable(false);
    view.set_cursor_visible(false);
    view.set_wrap_mode(gtk::WrapMode::WordChar);
    view.set_left_margin(4);
    view.set_right_margin(8);
    view.set_top_margin(4);
    view.set_bottom_margin(4);
    view.set_pixels_above_lines(1);
    view.set_pixels_below_lines(3);
    view.set_pixels_inside_wrap(2);
    view.add_css_class("markdown-view");
    view.set_accessible_role(gtk::AccessibleRole::Document);
    view
}

pub(crate) fn configure_source_buffer(buffer: &sourceview::Buffer, language_id: &str) {
    if !language_id.is_empty() {
        let manager = sourceview::LanguageManager::default();
        if let Some(language) = manager.language(language_id) {
            buffer.set_language(Some(&language));
            buffer.set_highlight_syntax(true);
        }
    }

    let style_manager = adw::StyleManager::default();
    let schemes = sourceview::StyleSchemeManager::default();
    let preferred = if style_manager.is_dark() {
        ["Adwaita-dark", "oblivion", "classic"]
    } else {
        ["Adwaita", "classic", "tango"]
    };
    if let Some(scheme) = preferred
        .into_iter()
        .find_map(|scheme_id| schemes.scheme(scheme_id))
    {
        buffer.set_style_scheme(Some(&scheme));
    }
}

fn install_tags(buffer: &gtk::TextBuffer) {
    add_tag(buffer, "strong", &[("weight", bold_weight_value())]);
    add_tag(
        buffer,
        "emphasis",
        &[("style", pango::Style::Italic.to_value())],
    );
    add_tag(
        buffer,
        "strikethrough",
        &[("strikethrough", true.to_value())],
    );
    add_tag(
        buffer,
        "superscript",
        &[
            ("rise", 5_000_i32.to_value()),
            ("scale", 0.82_f64.to_value()),
        ],
    );
    add_tag(
        buffer,
        "subscript",
        &[
            ("rise", (-3_000_i32).to_value()),
            ("scale", 0.82_f64.to_value()),
        ],
    );
    add_tag(
        buffer,
        "code-inline",
        &[
            ("family", "monospace".to_value()),
            ("scale", 0.94_f64.to_value()),
        ],
    );
    add_tag(
        buffer,
        "code-block",
        &[
            ("family", "monospace".to_value()),
            ("scale", 0.92_f64.to_value()),
            ("left-margin", 16_i32.to_value()),
            ("pixels-above-lines", 8_i32.to_value()),
            ("pixels-below-lines", 8_i32.to_value()),
        ],
    );
    add_tag(
        buffer,
        "quote",
        &[
            ("style", pango::Style::Italic.to_value()),
            ("left-margin", 20_i32.to_value()),
        ],
    );
    add_tag(
        buffer,
        "link",
        &[("underline", pango::Underline::Single.to_value())],
    );
    add_tag(
        buffer,
        "link-url",
        &[
            ("family", "monospace".to_value()),
            ("scale", 0.82_f64.to_value()),
        ],
    );
    add_tag(buffer, "list-marker", &[("weight", bold_weight_value())]);
    add_tag(buffer, "table-head", &[("weight", bold_weight_value())]);
    add_tag(buffer, "table-rule", &[("scale", 0.86_f64.to_value())]);

    for (name, scale) in [
        ("h1", 1.55_f64),
        ("h2", 1.38_f64),
        ("h3", 1.22_f64),
        ("h4", 1.12_f64),
        ("h5", 1.04_f64),
        ("h6", 1.0_f64),
    ] {
        add_tag(
            buffer,
            name,
            &[
                ("weight", bold_weight_value()),
                ("scale", scale.to_value()),
                ("pixels-above-lines", 10_i32.to_value()),
                ("pixels-below-lines", 4_i32.to_value()),
            ],
        );
    }
}

fn bold_weight_value() -> gtk::glib::Value {
    pango::Weight::Bold.into_glib().to_value()
}

fn add_tag(buffer: &gtk::TextBuffer, name: &str, properties: &[(&str, glib::Value)]) {
    let tag = gtk::TextTag::new(Some(name));
    for (property, value) in properties {
        tag.set_property_from_value(property, value);
    }
    buffer.tag_table().add(&tag);
}

fn insert(buffer: &gtk::TextBuffer, cursor: &mut gtk::TextIter, text: &str, tags: &[&str]) {
    if tags.is_empty() {
        buffer.insert(cursor, text);
    } else {
        buffer.insert_with_tags_by_name(cursor, text, tags);
    }
}

fn append_newlines(buffer: &gtk::TextBuffer, cursor: &mut gtk::TextIter, count: usize) {
    let current = buffer.text(&buffer.start_iter(), &buffer.end_iter(), false);
    let existing = current.chars().rev().take_while(|ch| *ch == '\n').count();
    for _ in existing..count {
        buffer.insert(cursor, "\n");
    }
}

fn ensure_block_break(buffer: &gtk::TextBuffer, cursor: &mut gtk::TextIter) {
    if buffer.char_count() > 0 {
        append_newlines(buffer, cursor, 2);
    }
}

fn ensure_line_start(buffer: &gtk::TextBuffer, cursor: &mut gtk::TextIter) {
    if buffer.char_count() > 0 && !at_line_start(buffer) {
        buffer.insert(cursor, "\n");
    }
}

fn at_line_start(buffer: &gtk::TextBuffer) -> bool {
    let text = buffer.text(&buffer.start_iter(), &buffer.end_iter(), false);
    text.is_empty() || text.ends_with('\n')
}

fn remove_last(tags: &mut Vec<&'static str>, tag: &'static str) {
    if let Some(index) = tags.iter().rposition(|candidate| *candidate == tag) {
        tags.remove(index);
    }
}

fn heading_tag(level: HeadingLevel) -> &'static str {
    match level {
        HeadingLevel::H1 => "h1",
        HeadingLevel::H2 => "h2",
        HeadingLevel::H3 => "h3",
        HeadingLevel::H4 => "h4",
        HeadingLevel::H5 => "h5",
        HeadingLevel::H6 => "h6",
    }
}

struct ListState {
    next: Option<u64>,
}

impl ListState {
    fn new(start: Option<u64>) -> Self {
        Self { next: start }
    }

    fn next_prefix(&mut self) -> String {
        match self.next.as_mut() {
            Some(next) => {
                let prefix = format!("{next}. ");
                *next += 1;
                prefix
            }
            None => "• ".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_prefixes_increment() {
        let mut list = ListState::new(Some(3));
        assert_eq!(list.next_prefix(), "3. ");
        assert_eq!(list.next_prefix(), "4. ");
    }

    #[test]
    fn text_tag_weight_uses_the_integer_property_type() {
        let value = bold_weight_value();
        assert_eq!(value.get::<i32>(), Ok(pango::Weight::Bold.into_glib()));
    }
}
