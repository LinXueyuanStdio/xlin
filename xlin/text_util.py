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

def pretty_limited_text(text: str, limited_length: int = 300, language="zh"):
    text = str(text).strip()
    if len(text) > limited_length:
        # if language == "zh":
        #     tail = f"...(共{len(text)}字)"
        # else:
        #     tail = f"...({len(text)} words in total)"
        # return text[: limited_length - len(tail)] + tail
        return text[: limited_length // 2] + text[-limited_length // 2 :]
    return text
