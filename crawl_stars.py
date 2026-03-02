import asyncio
import json
import os
import time
from dotenv import load_dotenv
import httpx

from db import postgres_fetch_all, postgres_insert, postgres_insert_many

load_dotenv()


def parse_api_response(json_response: dict):

    if not json_response:
        return None, None, None

    nodes = json_response.get("data", {}).get("search", {}).get("nodes", {})
    page_info = json_response.get("data", {}).get("search", {}).get("pageInfo", {})

    return nodes, page_info["hasNextPage"], page_info["endCursor"]


async def call_github(
    client: httpx.AsyncClient,
    min_stars: int,
    cursor: str | None = None,
):
    max_stars = min_stars + 9

    query = """
    query($cursor: String) {
        search(query: "stars:$$MIN_STARS$$..$$MAX_STARS$$", type: REPOSITORY, first: 100, after: $cursor) {
            repositoryCount
            pageInfo {
            hasNextPage
            endCursor
            }
            nodes {
            ... on Repository {
                id
                nameWithOwner
                stargazerCount
            }
            }
        }
    }
"""

    query: str = query.replace("$$MIN_STARS$$", str(min_stars))
    query: str = query.replace("$$MAX_STARS$$", str(max_stars))

    url = os.getenv("GRAPHQL_URL")
    token = os.getenv("GITHUB_PAT")

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    for i in range(
        3,
    ):
        try:
            res = await client.post(
                url=url,
                headers=headers,
                json={"query": query, "variables": {"cursor": cursor}},
            )

            if res.status_code == 429:
                reset_time = res.headers.get("x-ratelimit-reset")
                wait_time = max(int(reset_time) - int(time.time()), 1)

                print(f"Rate limits reached, waiting for {wait_time} seconds")

                await asyncio.sleep(wait_time)
                continue

            res.raise_for_status()

            await asyncio.sleep(0.5)

            data = res.json()

            return data
        except Exception as e:
            print(f"Error occurred while calling github: {e}")

            delay = (i + 1) ** 2  # 1, 4, 9 seconds of exp delay
            print(f"Sleeping for {delay} seconds before retry ...")
            await asyncio.sleep(delay)

    print(f"All retries failed, terminating flow.")
    return None


async def insert_into_db(repo_data: list[dict]):

    data = [(r["id"], r["nameWithOwner"], r["stargazerCount"]) for r in repo_data]

    query = """
    INSERT INTO repositories (node_id, repo_name, star_count, updated_at)
    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (node_id) 
    DO UPDATE SET 
        star_count = EXCLUDED.star_count,
        repo_name = EXCLUDED.repo_name,
        updated_at = EXCLUDED.updated_at;
    """

    res = await postgres_insert_many(query, data)
    return res


async def main():

    UPPER_BOUND = int(os.getenv("REPO_COUNT_UPPER"))
    count = 0
    min_stars = 0
    cursor = None

    repo_count = 0

    async with httpx.AsyncClient() as client:

        while True:
            count += 1

            print(f"Count: {count}")
            print()

            api_response = await call_github(client, min_stars=min_stars, cursor=cursor)
            new_nodes, hasNextPage, cursor = parse_api_response(api_response)

            if new_nodes == None:
                break

            db_res = await insert_into_db(new_nodes)

            if not db_res:
                print(f"Insert into database failed, breaking...")
                break

            repo_count += len(new_nodes)

            if repo_count >= UPPER_BOUND:
                print(f"There are {UPPER_BOUND} repos in the database, terminating...")
                break

            if not hasNextPage:
                print(f"Pages ended after iteration no. {count}")
                print(f"min stars: {min_stars}")
                print(f"Cursor: {cursor}")
                print(f"HasNextPage: {hasNextPage}")

                min_stars += 10

                print(f"Updating min stars count to {min_stars}")
                print()


asyncio.run(main())
