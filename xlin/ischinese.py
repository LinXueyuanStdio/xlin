def text_is_all_chinese(test: str):
    for ch in test:
        if '\u4e00' <= ch <= '\u9fff':
            continue
        return False
    return True


def text_contains_chinese(test: str):
    for ch in test:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False
