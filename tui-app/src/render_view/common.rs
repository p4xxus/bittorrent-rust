use ratatui::layout::{Constraint, Flex, Layout, Rect};

pub(super) fn center(area: Rect, horizontal: Constraint, vertical: Constraint) -> Rect {
    let area = center_vertical(area, vertical);
    center_horizontal(area, horizontal)
}

pub(super) fn center_vertical(area: Rect, height: Constraint) -> Rect {
    let [area] = Layout::vertical([height]).flex(Flex::Center).areas(area);
    area
}

pub(super) fn center_horizontal(area: Rect, width: Constraint) -> Rect {
    let [area] = Layout::horizontal([width]).flex(Flex::Center).areas(area);
    area
}
