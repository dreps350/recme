import pandas as pd


TOPICS = {
    "живопись": {"масло", "картина", "живопись", "художник", "акварель", "акрил", "рисование"},
    "фотография": {"фотография", "фотограф", "фотосъёмка", "фотосессия"},
    "лепка": {"глина", "лепка", "полимерный", "лепить"},
    "ювелирное_дело": {"ювелирный", "бижутерия", "ювелир", "брошь"},
    "кулинария": {"торт", "пряник", "десерт", "кулинарный", "кондитерский", "кухня", "еда", "дегустация", "повар"},
    "флористика": {"флористика", "букет", "цветок", "цветочный", "флорист", "флорариум"},
    "красота_и_здоровье": {"макияж", "стилист", "визажист", "стрижка"},
    "дети": {"ребёнок", "детский"},
    "музыка": {"концерт", "музыка", "концертный", "клип", "песня", "кавер"},
    "вязание": {"вязание", "вязаный", "крючок", "спица", "пряжа"},
    "шитье": {"шить", "нитки"},
    "валяние": {"валяние"},
    "пэтчворк": {"пэчворк"},
    "декупаж": {"декупаж"},
}


def map_tag_topics(tags: "set"):
    res = set()
    for tpc, map_tags in TOPICS.items():
        if map_tags & tags:
            res.add(tpc)
    return res


def map_text_topics(text):
    res = set()
    for tpc, words in TOPICS.items():
        for w in words:
            if w in text:
                res.add(tpc)
    return res


def map_topics_set(tokens: set):
    res = {key: [] for key in TOPICS}
    for tpc, words in TOPICS.items():
        if tokens & words:
            res[tpc].append(1)
        else:
            res[tpc].append(0)
    return pd.DataFrame(res)


def map_topics_list(tokens: list):
    res = {key: [] for key in TOPICS}
    for tpc, words in TOPICS.items():
        for w in words:
            if w in tokens and (w not in res[tpc]):
                res[tpc].append(1)
            else:
                res[tpc].append(0)
    return pd.DataFrame(res)
