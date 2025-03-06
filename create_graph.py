import json
import logging
import os
import time
import traceback
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from neo4j import Driver, GraphDatabase

from bangumi_common.py.platform import PLATFORM_CONFIG, Platform
from raw_data_reader import SUBJECT_PERSON_CONFIG, SUBJECT_RELATION_CONFIG

load_dotenv()

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")  # Default to local
USERNAME = os.environ.get("NEO4J_USERNAME", "neo4j")
PASSWORD = os.environ.get("NEO4J_PASSWORD", "bangumibot")

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,  # Default level for all handlers
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console (inherits level from basicConfig)
    ],
)

# Create a file handler with WARNING level
file_handler = logging.FileHandler("local/bangumi_database.log", mode="w")
file_handler.setLevel(logging.WARNING)
# Set the same format as the basicConfig
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
# Get the root logger and add the file handler
logger = logging.getLogger(__name__)
logger.addHandler(file_handler)

ENTITY_LIMIT = None
RELATION_LIMIT = None

CATEGORY_MAPPING = {
    1: "书籍",
    2: "动画",
    3: "音乐",
    4: "游戏",
    6: "三次元",
}

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

SUBJECT_CHARACTER_TYPE_MAPPING = {
    1: "主角",
    2: "配角",
    3: "客串",
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


@dataclass
class SubjectPersonRelation:
    person_id: int
    subject_id: int
    position: int


@dataclass
class SubjectCharacterRelation:
    character_id: int
    subject_id: int
    type: int


@dataclass
class PersonCharacterRelation:
    person_id: int
    character_id: int
    subject_id: int


class BangumiDatabase:
    def __init__(self, driver: Driver):
        self.driver = driver
        self.subject_category_mapping = {}
        self.subject_name_mapping = {}
        self.person_id_set = set()
        self.character_name_mapping = {}

    def close(self) -> None:
        self.driver.close()
        logger.info("Closed the Neo4j driver session.")

    def clear_database(self) -> None:
        self.person_id_set = set()

        logger.info("Clearing database...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared.")

    def _initliaze_constraints(self) -> None:
        logger.info("Initializing constraints.")
        with self.driver.session() as session:
            result = session.run(
                "SHOW CONSTRAINTS YIELD name WHERE name = 'unique_subject_id' RETURN count(*) AS constraint_exists;"
            )
            if result.value()[0] == 0:
                session.run(
                    "CREATE CONSTRAINT unique_subject_id FOR (s:Subject) REQUIRE s.subject_id IS UNIQUE;"
                )
            result = session.run(
                "SHOW CONSTRAINTS YIELD name WHERE name = 'unique_person_id' RETURN count(*) AS constraint_exists;"
            )
            if result.value()[0] == 0:
                session.run(
                    "CREATE CONSTRAINT unique_person_id FOR (p:Person) REQUIRE p.person_id IS UNIQUE;"
                )
            result = session.run(
                "SHOW CONSTRAINTS YIELD name WHERE name = 'unique_character_id' RETURN count(*) AS constraint_exists;"
            )
            if result.value()[0] == 0:
                session.run(
                    "CREATE CONSTRAINT unique_character_id FOR (c:Character) REQUIRE c.character_id IS UNIQUE;"
                )

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

        self.subject_category_mapping[subject.id] = subject.type
        self.subject_name_mapping[subject.id] = subject.name

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
                MERGE (s)-[:BELONG_TO]->(p)
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

        self.person_id_set.add(person.id)

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
        self.character_name_mapping[character.id] = character.name
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
            self.subject_category_mapping[subject_relation.related_subject_id]
        ]
        if subject_relation.relation_type not in category_relations:
            relation_name = CATEGORY_MAPPING[
                self.subject_category_mapping[subject_relation.related_subject_id]
            ]
        else:
            relation_name = category_relations[subject_relation.relation_type].cn
        with self.driver.session() as session:
            session.run(
                """
                MATCH (s1:Subject {subject_id: $subject_id})
                MATCH (s2:Subject {subject_id: $related_subject_id})
                MERGE (s1)-[:SubjectRelation {type: $relation_type}]->(s2)
                """,
                subject_id=subject_relation.subject_id,
                related_subject_id=subject_relation.related_subject_id,
                relation_type=relation_name,
            )

    def _insert_a_subject_person_relation(
        self, subject_person_relation: SubjectPersonRelation
    ) -> None:
        category_relations = SUBJECT_PERSON_CONFIG[
            self.subject_category_mapping[subject_person_relation.subject_id]
        ]
        relation_name = category_relations[subject_person_relation.position].cn
        logger.info(
            f"Inserting subject-person relation for subject {subject_person_relation.subject_id}"
            + f" to person {subject_person_relation.person_id} into database with position {relation_name}.",
        )
        with self.driver.session() as session:
            session.run(
                """
                MATCH (s:Subject {subject_id: $subject_id})
                MATCH (p:Person {person_id: $person_id})
                MERGE (s)-[:SubjectPersonRelation {type: $relation_type}]->(p)
                """,
                subject_id=subject_person_relation.subject_id,
                person_id=subject_person_relation.person_id,
                relation_type=relation_name,
            )

    def _insert_a_subject_character_relation(
        self, subject_character_relation: SubjectCharacterRelation
    ) -> None:
        logger.info(
            f"Inserting subject-character relation for subject {subject_character_relation.subject_id}"
            + f" and character {subject_character_relation.character_id} into database.",
        )
        with self.driver.session() as session:
            session.run(
                """
                MATCH (c:Character {character_id: $character_id})
                MATCH (s:Subject {subject_id: $subject_id})
                MERGE (c)-[:AppearsIn {type: $type}]->(s)
                """,
                character_id=subject_character_relation.character_id,
                subject_id=subject_character_relation.subject_id,
                type=SUBJECT_CHARACTER_TYPE_MAPPING[subject_character_relation.type],
            )

    def _insert_a_person_character_relation(
        self, person_character_relation: PersonCharacterRelation
    ) -> None:
        logger.info(
            f"Inserting person-character relation for person {person_character_relation.person_id}"
            + f" and character {person_character_relation.character_id} in subject "
            + f"{person_character_relation.subject_id} into database.",
        )
        with self.driver.session() as session:
            session.run(
                """
                MATCH (p:Person {person_id: $person_id}) 
                MATCH (c:Character {character_id: $character_id}) 
                MATCH (s:Subject {subject_id: $subject_id}) 
                MERGE (p)-[:Played]->(r:RolePerformance {role: $character_name + " in " + $subject_name})
                MERGE (r)-[:AsCharacter]->(c)
                MERGE (r)-[:In]->(s);
                """,
                person_id=person_character_relation.person_id,
                character_id=person_character_relation.character_id,
                subject_id=person_character_relation.subject_id,
                character_name=self.character_name_mapping[
                    person_character_relation.character_id
                ],
                subject_name=self.subject_name_mapping[
                    person_character_relation.subject_id
                ],
            )

    def initilize_database(self, data_folder: Path = Path("raw_data")) -> None:
        logger.info("Initializing database.")
        total_start = time.time()

        time_records = {}

        # Clear Database
        start = time.time()
        self.clear_database()
        time_records["Clear Database"] = time.time() - start

        # Initialize platforms
        start = time.time()
        for category, item_list in PLATFORM_CONFIG.items():
            for platform in item_list.values():
                self._insert_a_platform(platform, category)
        time_records["Initialize Platforms"] = time.time() - start

        # Initialize Constraints
        start = time.time()
        self._initliaze_constraints()
        time_records["Initialize Constraints"] = time.time() - start

        # Initialize Subjects
        start = time.time()
        logger.info("Inserting subjects from file.")
        with open(data_folder / "subject.jsonlines", "r", encoding="utf-8") as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                subject = Subject(
                    **{k: v for k, v in data.items() if k in Subject.__annotations__}
                )
                self._insert_a_subject(subject)
                cnt += 1
                if ENTITY_LIMIT is not None and cnt >= ENTITY_LIMIT:
                    break
        time_records["Insert Subjects"] = time.time() - start

        # Initialize Persons
        start = time.time()
        logger.info("Inserting persons from file.")
        with open(data_folder / "person.jsonlines", "r", encoding="utf-8") as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                person = Person(
                    **{k: v for k, v in data.items() if k in Person.__annotations__}
                )
                self._insert_a_person(person)
                cnt += 1
                if ENTITY_LIMIT is not None and cnt >= ENTITY_LIMIT:
                    break
        time_records["Insert Persons"] = time.time() - start

        # Initialize Characters
        start = time.time()
        logger.info("Inserting characters from file.")
        with open(data_folder / "character.jsonlines", "r", encoding="utf-8") as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                character = Character(
                    **{k: v for k, v in data.items() if k in Character.__annotations__}
                )
                self._insert_a_character(character)
                cnt += 1
                if ENTITY_LIMIT is not None and cnt >= ENTITY_LIMIT:
                    break
        time_records["Insert Characters"] = time.time() - start

        # Initialize Subject Relations
        start = time.time()
        logger.info("Inserting subject relations from file.")
        with open(
            data_folder / "subject-relations.jsonlines", "r", encoding="utf-8"
        ) as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                subject_relation = SubjectRelation(
                    **{
                        k: v
                        for k, v in data.items()
                        if k in SubjectRelation.__annotations__
                    }
                )
                if (
                    subject_relation.subject_id in self.subject_category_mapping
                    and subject_relation.related_subject_id
                    in self.subject_category_mapping
                ):
                    try:
                        self._insert_a_subject_relation(subject_relation)
                    except Exception:
                        traceback.print_exc()
                        logger.error(
                            f"Error inserting subject relation: {subject_relation.subject_id} to {subject_relation.related_subject_id}"
                        )
                    cnt += 1
                    if RELATION_LIMIT is not None and cnt >= RELATION_LIMIT:
                        break
        time_records["Insert Subject Relations"] = time.time() - start

        # Initialize Subject-Person Relations
        start = time.time()
        logger.info("Inserting subject-person relations from file.")
        with open(
            data_folder / "subject-persons.jsonlines", "r", encoding="utf-8"
        ) as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                subject_person_relation = SubjectPersonRelation(
                    **{
                        k: v
                        for k, v in data.items()
                        if k in SubjectPersonRelation.__annotations__
                    }
                )
                if (
                    subject_person_relation.subject_id in self.subject_category_mapping
                    and subject_person_relation.person_id in self.person_id_set
                ):
                    try:
                        self._insert_a_subject_person_relation(subject_person_relation)
                    except Exception:
                        traceback.print_exc()
                        logger.error(
                            f"Error inserting subject-person relation: {subject_person_relation.subject_id} to {subject_person_relation.person_id}"
                        )
                    cnt += 1
                    if RELATION_LIMIT is not None and cnt >= RELATION_LIMIT:
                        break
        time_records["Insert Subject-Person Relations"] = time.time() - start

        # Initialize Subject-Character Relations
        start = time.time()
        logger.info("Inserting subject-character relations from file.")
        with open(
            data_folder / "subject-characters.jsonlines", "r", encoding="utf-8"
        ) as f:
            cnt = 0
            for line in f:
                data = json.loads(line)
                subject_character_relation = SubjectCharacterRelation(
                    **{
                        k: v
                        for k, v in data.items()
                        if k in SubjectCharacterRelation.__annotations__
                    }
                )
                if (
                    subject_character_relation.subject_id
                    in self.subject_category_mapping
                    and subject_character_relation.character_id
                    in self.character_name_mapping
                ):
                    try:
                        self._insert_a_subject_character_relation(
                            subject_character_relation
                        )
                    except Exception:
                        traceback.print_exc()
                        logger.error(
                            f"Error inserting subject-character relation: {subject_character_relation.subject_id} to {subject_character_relation.character_id}"
                        )
                    cnt += 1
                    if RELATION_LIMIT is not None and cnt >= RELATION_LIMIT:
                        break
        time_records["Insert Subject-Character Relations"] = time.time() - start

        # Final time logging
        total_time = time.time() - total_start
        logger.info("Database initialization completed. Time summary:")
        for section, duration in time_records.items():
            logger.info(f"{section}: {duration:.2f} seconds")
        logger.info(f"Total time: {total_time:.2f} seconds")


if __name__ == "__main__":
    driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))
    logger.info("Connected to Neo4j database.")
    db = BangumiDatabase(driver)
    db.initilize_database()
    db.close()
