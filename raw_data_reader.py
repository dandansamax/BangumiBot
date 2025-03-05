import yaml
import msgspec
from pathlib import Path

__common_path = Path(__file__).parent.joinpath("bangumi_common").resolve()

class SubjectRelation(msgspec.Struct):
    en: str
    cn: str
    jp: str
    desc: str
    skip_vice_versa: bool = False

__subject_relations = yaml.safe_load(
    __common_path.joinpath("subject_relations.yml").read_bytes()
)

SUBJECT_RELATION_CONFIG: dict[int, dict[int, SubjectRelation]] = msgspec.convert(
    {
        key: value
        for key, value in __subject_relations["relations"].items()
        if isinstance(key, int)
    },
    type=dict[int, dict[int, SubjectRelation]],
)

class SubjectPerson(msgspec.Struct):
    en: str
    cn: str
    jp: str
    rdf: str = ""
    desc: str = ""


__subject_staffs = yaml.safe_load(
    __common_path.joinpath("subject_staffs.yml").read_bytes()
)

SUBJECT_PERSON_CONFIG: dict[int, dict[int, SubjectPerson]] = msgspec.convert(
    {
        key: value
        for key, value in __subject_staffs["staffs"].items()
        if isinstance(key, int)
    },
    type=dict[int, dict[int, SubjectPerson]],
)
