import argparse
import asyncio
import dataclasses
import json
import logging
from datetime import datetime
from typing import Any, Callable, List

from playwright.async_api import async_playwright, Browser, ElementHandle, Page


@dataclasses.dataclass
class RedditComment:
    id: str
    text: str
    created_at: str


@dataclasses.dataclass
class RedditPost:
    id: str
    subreddit: str
    title: str
    created_at: str
    comments: List[RedditComment]


def with_page(browser: Browser) -> Callable:
    def decorator(function: Callable) -> Callable:
        async def wrapper(*args, **kwargs) -> Any:
            page = await browser.new_page()
            try:
                return await function(page=page, *args, **kwargs)
            finally:
                await page.close()

        return wrapper
    return decorator


def with_semaphore(semaphore: asyncio.Semaphore) -> Callable:
    def decorator(function: Callable) -> Callable:
        async def wrapper(*args, **kwargs) -> Any:
            async with semaphore:
                return await function(*args, **kwargs)

        return wrapper
    return decorator


def retry(logger: logging.Logger, max_attempts: int = 1) -> Any:
    def decorator(function: Callable) -> Callable:
        async def wrapper(*args, **kwargs) -> Any:
            if max_attempts < 1:
                raise Exception("Argument 'max_attempts' must be poritive")

            attempt = 0
            exception = None
            while attempt < max_attempts:
                attempt += 1
                try:
                    return await function(*args, **kwargs)
                except Exception as exc:
                    logger.warning("Attempt=%d; Error: %s", attempt, exc)
                    exception = exc
            
            raise exception
        
        return wrapper
    return decorator


def stop_raise(logger: logging.Logger) -> Callable:
    def decorator(function: Callable) -> Callable:
        async def wrapper(*args, **kwargs) -> Any:
            try:
                return await function(*args, **kwargs)
            except Exception as exc:
                logger.error("Get exception: %s", exc)
        
        return wrapper
    return decorator


class RedditScrapper:
    BASE_URL: str = "https://reddit.com"
    
    POST_SELECTOR: str = ".Post"
    POST_SUBREDDIT_SELECTOR: str = "._3ryJoIoycVkA88fy40qNJc"
    POST_TITLE_SELECTOR: str = ".SQnoC3ObvgnGjWt90zD9Z"
    POST_CREATED_AT_SELECTOR: str = "._2VF2J19pUIMSLJFky-7PEI"
    POST_CREATED_AT_POPUP_SELECTOR: str = (
        "._2J_zB4R1FH2EjGMkQjedwc.u6HtAZu8_LKL721-EnKuR[data-popper-reference-hidden='false']"
    )

    COMMENT_SELECTOR: str = "._3sf33-9rVAO_v4y0pIW_CH"
    COMMENT_TEXT_SELECTOR: str = "._1qeIAgB0cPwnLhDF9XSiJM"
    COMMENT_CREATED_AT_SELECTOR: str = "._3yx4Dn0W3Yunucf5sVJeFU"
    COMMENT_CREATED_AT_POPUP_SELECTOR: str = ".HQ2VJViRjokXpRbJzPvvc"

    SCROLL_TRIES: int = 3
    SCROLL_DELAY_SECONDS: float = 5
    MAX_CONCURRENT_TASK: int = 1
    TIMEOUT_MS: int = 10000

    @staticmethod
    def to_isoformat(value: str) -> str:
        result = " ".join(value.split()[:-3])
        result = datetime.strptime(result, "%a, %b %d, %Y, %I:%M:%S %p").isoformat()

        return result

    def __init__(self, *keywords: str, debug: bool = False):
        self.browser: Browser | None = None
        self.semaphore = asyncio.Semaphore(value=self.MAX_CONCURRENT_TASK)
        self.query = " ".join(keywords)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

    async def run(self) -> List[RedditPost]:
        async with async_playwright() as playwright:
            self.browser = await playwright.chromium.launch()
            
            coroutine = self.search
            coroutine = with_page(browser=self.browser)(coroutine)
            return await coroutine()

    async def search(self, page: Page) -> List[RedditPost]:
        await page.goto(url=f"{self.BASE_URL}/search?q={self.query}")
        self.logger.debug("Load search page.")

        posts = []
        post_ids = set()
        tasks = []
        have_new_posts = True
        tries = 0

        while have_new_posts or tries < self.SCROLL_TRIES:
            if not have_new_posts:
                self.logger.debug("Have no found new posts, attempt %d", tries + 1)

            have_new_posts = False
            tries += 1

            element_handles = await page.locator(self.POST_SELECTOR).element_handles()
            element_handles = element_handles[len(posts):]
            for element_handle in element_handles:
                await element_handle.scroll_into_view_if_needed(timeout=self.TIMEOUT_MS)

                coroutine = self.parse_post
                coroutine = stop_raise(logger=self.logger)(coroutine)
                post = await coroutine(page=page, element_handle=element_handle)
                if post is None:
                    self.logger.error("Cannot parse post")
                    continue
                if (post.subreddit, post.id) in post_ids:
                    self.logger.warning("Post (%s, %s) already scrapped.", post.subreddit, post.id)
                    continue

                tries = 0
                have_new_posts = True
                post_ids.add((post.subreddit, post.id))
                posts.append(post)

                coroutine = self.search_comments
                coroutine = with_page(browser=self.browser)(coroutine)
                coroutine = retry(logger=self.logger, max_attempts=3)(coroutine)
                coroutine = with_semaphore(semaphore=self.semaphore)(coroutine)
                task = asyncio.create_task(coroutine(post=post))
                tasks.append(task)
        
            self.logger.info("Found %d reddit posts.", len(posts))
            await asyncio.sleep(self.SCROLL_DELAY_SECONDS)

        self.logger.info("All posts loaded.")
        if tasks:
            await asyncio.wait(tasks)
        self.logger.info("All comments loaded.")
        return posts

    async def parse_post(self, page: Page, element_handle: ElementHandle) -> RedditPost:
        id = await element_handle.get_attribute("id")
        id = id.lstrip("t3_")
        
        subreddit = await element_handle.query_selector(self.POST_SUBREDDIT_SELECTOR)
        subreddit = await subreddit.inner_text()

        title = await element_handle.query_selector(self.POST_TITLE_SELECTOR)
        title = await title.inner_text()

        created_at_element = await element_handle.query_selector(self.POST_CREATED_AT_SELECTOR)
        await created_at_element.hover(timeout=self.TIMEOUT_MS)
        await page.wait_for_selector(self.POST_CREATED_AT_POPUP_SELECTOR, timeout=self.TIMEOUT_MS)
        created_at = await page.locator(self.POST_CREATED_AT_POPUP_SELECTOR).element_handle()
        created_at = await created_at.inner_text()
        created_at = self.to_isoformat(value=created_at)

        return RedditPost(
            id=id,
            title=title,
            subreddit=subreddit,
            created_at=created_at,
            comments=[],
        )

    async def search_comments(self, page: Page, post: RedditPost) -> List[RedditComment]:
        await page.goto(url=f"{self.BASE_URL}/{post.subreddit}/comments/{post.id}")
        self.logger.debug("Load post page: subreddit=%s; id=%s.", post.subreddit, post.id)

        comments = []
        comment_ids = set()
        have_new_comments = True
        tries = 0

        while have_new_comments or tries < self.SCROLL_TRIES:
            if not have_new_comments:
                self.logger.debug(
                    "Have no found new comments for post (subreddit=%s; id=%s), attempt %d",
                    post.subreddit,
                    post.id,
                    tries + 1,
                )

            have_new_comments = False
            tries += 1

            element_handles = await page.locator(self.COMMENT_SELECTOR).element_handles()
            element_handles = element_handles[len(comments):]
            for element_handle in element_handles:
                await element_handle.scroll_into_view_if_needed(timeout=self.TIMEOUT_MS)

                coroutine = self.parse_comment
                coroutine = stop_raise(logger=self.logger)(coroutine)
                comment = await coroutine(page=page, element_handle=element_handle)
                if comment is None:
                    self.logger.error(
                        "Cannot parse comment on (subreddit=%s; id=%s) post",
                        post.subreddit,
                        post.id,
                    )
                    continue
                if comment.id in comment_ids:
                    self.logger.warning(
                        "Comment (subreddit=%s; post_id=%s, id=%s) already scrapped.",
                        post.subreddit,
                        post.id,
                        comment.id,
                    )
                    continue

                tries = 0
                have_new_comments = True
                comment_ids.add(comment.id)
                comments.append(comment)

            self.logger.info("Found %d comments for post (subreddit=%s; id=%s)", len(comments),
                             post.subreddit, post.id)
            await asyncio.sleep(self.SCROLL_DELAY_SECONDS)

        post.comments = comments
        return comments

    async def parse_comment(self, page: Page, element_handle: ElementHandle) -> RedditComment:
        id = await element_handle.get_attribute("id")
        id = id.lstrip("t1_")

        text = await element_handle.query_selector(self.COMMENT_TEXT_SELECTOR)
        text = "" if text is None else await text.inner_text()

        created_at_element = await element_handle.query_selector(self.COMMENT_CREATED_AT_SELECTOR)
        await created_at_element.hover(timeout=self.TIMEOUT_MS)
        await page.wait_for_selector(
            self.COMMENT_CREATED_AT_POPUP_SELECTOR,
            timeout=self.TIMEOUT_MS,
        )
        created_at = await page.locator(self.COMMENT_CREATED_AT_POPUP_SELECTOR).element_handle()
        created_at = await created_at.inner_text()
        created_at = self.to_isoformat(value=created_at)

        return RedditComment(id=id, text=text, created_at=created_at)


def run(start_datetime: int, *keywords: str, debug: bool = False) -> str:
    scrapper = RedditScrapper(*keywords, debug=debug)
    posts = asyncio.run(scrapper.run())
    posts = [dataclasses.asdict(post) for post in posts]

    return json.dumps(posts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(
        prog="Exorde Reddit Scrapper",
        description="Exorde network module for scrapping posts and comments from Reddit",
    )
    parser.add_argument("keywords", type=str, nargs="+", help="Search keywords.")
    parser.add_argument("-o", "--output", type=str, default="output.json", help="Output file name.")
    args = parser.parse_args()

    output = run(1, *args.keywords, debug=True)
    with open(args.output, "wt") as output_file:
        output_file.write(output)
