import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
import traceback

from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase

from bangumi_common.py.platform import PLATFORM_CONFIG, Platform
from raw_data_reader import SUBJECT_RELATION_CONFIG

load_dotenv()

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")  # Default to local
USERNAME = os.environ.get("NEO4J_USERNAME", "neo4j")
PASSWORD = os.environ.get("NEO4J_PASSWORD", "bangumibot")

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,  # Set level to INFO, DEBUG for more detailed logs
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console
        # logging.FileHandler("bangumi_database.log", mode="w"),  # Log to a file
    ],
)
logger = logging.getLogger(__name__)

SUBJECT_LIMIT = 800
PERSON_LIMIT = 800
CHARACTER_LIMIT = 800
SUBJECT_RELATION_LIMIT = 800

CATEGORY_MAPPING = {
    1: "书籍",
    2: "动画",
    3: "音乐",
    4: "游戏",
    6: "三次元",
}

SUBJECT_CATEGORY_MAPPING = {}

PERSON_TYPE_MAPPING = {1: "个人", 2: "公司", 3: "组合"}

CAREER_MAPPING = {
    "producer": "制作人员",
    "writer": "作家",
    "actor": "演员",
    "illustrator": "绘师",
    "seiyu": "声优",
    "mangaka": "漫画家",
    "artist": "音乐人",
}

CHARACTER_ROLE_MAPPING = {
    1: "角色",
    2: "机体",
    3: "组织",
    4: "未知",
}


@dataclass
class Subject:
    id: int
    type: int
    name: str
    name_cn: str
    infobox: str
    platform: int
    summary: str
    nsfw: bool
    date: str
    series: bool
    tags: dict[str, int]


@dataclass
class Person:
    id: int
    name: str
    type: int
    infobox: str
    summary: str
    career: list[str]


@dataclass
class Character:
    id: int
    role: int
    name: str
    infobox: str
    summary: str


@dataclass
class SubjectRelation:
    subject_id: int
    related_subject_id: int
    relation_type: int


class BangumiDatabase:
    def __init__(self, driver: Driver):
        self.driver = driver

    def close(self) -> None:
        self.driver.close()
        logger.info("Closed the Neo4j driver session.")

    def clear_database(self) -> None:
        logger.info("Clearing database...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared.")

    def _insert_a_platform(self, platform: Platform, category: int) -> None:
        name = CATEGORY_MAPPING[category] + "/" + platform.type_cn
        logger.info(f"Inserting platform: {name}.")
        with self.driver.session() as session:
            session.run(
                "CREATE (:Platform {platform_id: $id, category: $category, name: $name})",
                id=platform.id,
                category=category,
                name=name,
            )

    def _insert_a_subject(self, subject: Subject) -> None:
        if subject.nsfw:
            return

        SUBJECT_CATEGORY_MAPPING[subject.id] = subject.type

        logger.info(f"Inserting subject {subject.name} into database.")
        with self.driver.session() as session:
            session.run(
                """
                CREATE (s:Subject {
                    subject_id: $id,
                    name: $name,
                    name_cn: $name_cn,
                    infobox: $infobox,
                    summary: $summary,
                    date: $date,
                    series: $series,
                    tags: $tags
                })

                MERGE (p:Platform {platform_id: $platform_id, category: $category})
                CREATE (s)-[:BELONG_TO]->(p)
                """,
                id=subject.id,
                name=subject.name,
                name_cn=subject.name_cn,
                infobox=subject.infobox,  # TODO: parse infobox as dict
                summary=subject.summary,
                date=subject.date,
                series=subject.series,
                tags=[item["name"] for item in subject.tags],
                platform_id=subject.platform,
                category=subject.type,
            )

    def _insert_a_person(self, person: Person):
        logger.info(f"Inserting person {person.name} into database.")
        if person.type == 0:
            logger.warning(f"Person {person.name} with id {person.id} has no type.")
            return
        with self.driver.session() as session:
            session.run(
                """
                CREATE (p:Person {
                    person_id: $id,
                    name: $name,
                    type: $type,
                    infobox: $infobox,
                    summary: $summary,
                    career: $career
                })
                """,
                id=person.id,
                name=person.name,
                type=PERSON_TYPE_MAPPING[person.type],
                infobox=person.infobox,  # TODO: parse infobox
                summary=person.summary,
                career=[CAREER_MAPPING[career] for career in person.career],
            )

    def _insert_a_character(self, character: Character):
        logger.info(f"Inserting character {character.name} into database.")
        with self.driver.session() as session:
            session.run(
                """
                CREATE (c:Character {
                    character_id: $id,
                    role: $role,
                    name: $name,
                    infobox: $infobox,
                    summary: $summary
                })
                """,
                id=character.id,
                role=CHARACTER_ROLE_MAPPING[character.role],
                name=character.name,
                infobox=character.infobox,
                summary=character.summary,
            )

    def _insert_a_subject_relation(self, subject_relation: SubjectRelation) -> None:
        logger.info(
            f"Inserting subject relation for {subject_relation.subject_id}"
            + f" to {subject_relation.related_subject_id} into database.",
        )
        category_relations = SUBJECT_RELATION_CONFIG[
            SUBJECT_CATEGORY_MAPPING[subject_relation.related_subject_id]
        ]
        if subject_relation.relation_type not in category_relations:
            relation_name = CATEGORY_MAPPING[
                SUBJECT_CATEGORY_MAPPING[subject_relation.related_subject_id]
            ]
        else:
            relation_name = category_relations[subject_relation.relation_type].cn
        with self.driver.session() as session:
            session.run(
                """
                MATCH (s1:Subject {subject_id: $subject_id})
                MATCH (s2:Subject {subject_id: $related_subject_id})
                CREATE (s1)-[:SubjectRelation {type: $relation_type}]->(s2)
                """,
                subject_id=subject_relation.subject_id,
                related_subject_id=subject_relation.related_subject_id,
                relation_type=relation_name,
            )

    def initilize_database(self, data_folder: Path = Path("raw_data")) -> None:
        logger.info("Initializing database.")
        self.clear_database()

        # Initialize platforms
        for category, item_list in PLATFORM_CONFIG.items():
            for platform in item_list.values():
                self._insert_a_platform(platform, category)

        # Initialize subjects
        logger.info("Inserting subjects from file.")
        with open(data_folder / "subject.jsonlines", "r", encoding="utf-8") as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                # Create Subject instance while ignoring missing keys
                subject = Subject(
                    **{k: v for k, v in data.items() if k in Subject.__annotations__}
                )
                self._insert_a_subject(subject)
                cnt += 1
                if SUBJECT_LIMIT is not None and cnt >= SUBJECT_LIMIT:
                    break
        logger.info("Subject insertion completed.")

        # Initialize Persons
        logger.info("Inserting persons from file.")
        with open(data_folder / "person.jsonlines", "r", encoding="utf-8") as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                # Create Subject instance while ignoring missing keys
                person = Person(
                    **{k: v for k, v in data.items() if k in Person.__annotations__}
                )
                self._insert_a_person(person)
                cnt += 1
                if PERSON_LIMIT is not None and cnt >= PERSON_LIMIT:
                    break
        logger.info("Person insertion completed.")

        # Initialize Characters
        logger.info("Inserting characters from file.")
        with open(data_folder / "character.jsonlines", "r", encoding="utf-8") as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                # Create Subject instance while ignoring missing keys
                character = Character(
                    **{k: v for k, v in data.items() if k in Character.__annotations__}
                )
                self._insert_a_character(character)
                cnt += 1
                if CHARACTER_LIMIT is not None and cnt >= CHARACTER_LIMIT:
                    break

        # Initialize Subject Relations
        logger.info("Inserting subject relations from file.")
        with open(
            data_folder / "subject-relations.jsonlines", "r", encoding="utf-8"
        ) as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                # Create Subject instance while ignoring missing keys
                subject_relation = SubjectRelation(
                    **{
                        k: v
                        for k, v in data.items()
                        if k in SubjectRelation.__annotations__
                    }
                )
                if (
                    subject_relation.subject_id in SUBJECT_CATEGORY_MAPPING
                    and subject_relation.related_subject_id in SUBJECT_CATEGORY_MAPPING
                ):
                    try:
                        self._insert_a_subject_relation(subject_relation)
                    except Exception as e:
                        traceback.print_exc()
                        logger.error(
                            f"Error inserting subject relation: {subject_relation.subject_id} to {subject_relation.related_subject_id}"
                        )
                    cnt += 1
                    if (
                        SUBJECT_RELATION_LIMIT is not None
                        and cnt >= SUBJECT_RELATION_LIMIT
                    ):
                        break
        logger.info("Subject relation insertion completed.")


if __name__ == "__main__":
    driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))
    logger.info("Connected to Neo4j database.")
    db = BangumiDatabase(driver)
    db.initilize_database()
    db.close()
