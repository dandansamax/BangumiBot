import json
import logging
from neo4j import Driver, GraphDatabase
from bangumi_common.py.platform import Platform, PLATFORM_CONFIG
from dataclasses import dataclass
from pathlib import Path

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
                infobox=person.infobox, #TODO: parse infobox
                summary=person.summary,
                career=[CAREER_MAPPING[career] for career in person.career],
            )

    # def _insert_a_character(self, character: Character):
    #     pass

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


if __name__ == "__main__":
    NEO4J_URI = "bolt://localhost:7687"  # Change if using a remote server
    USERNAME = "neo4j"
    PASSWORD = "bangumibot"
    driver = GraphDatabase.driver(NEO4J_URI, auth=(USERNAME, PASSWORD))
    logger.info("Connected to Neo4j database.")
    db = BangumiDatabase(driver)
    try:
        db.initilize_database()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        db.close()
