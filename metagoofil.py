#!/usr/bin/env python3
"""
Metagoofil - Search a domain for public documents and download/list them.

This is a patched fork of the original metagoofil (opsdisk/metagoofil).
Google search (via the `googlesearch` library) was broken because Google
started blocking scripted queries against that library. This version swaps
the search backend to a multi-engine metasearch (via the `ddgs` library,
covering DuckDuckGo, Bing, Brave, Yahoo, Mojeek and Startpage), which still
supports `site:` and `filetype:` search operators and falls back across
engines automatically if one is rate-limiting. Everything else (threading
model, download logic, CLI flags, output format) behaves the same way as
the original for the end user.

Original project: https://github.com/opsdisk/metagoofil
"""

import argparse
import os
import queue
import random
import sys
import threading
import time
import urllib.parse

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# New search backend (replaces `googlesearch`)
from ddgs import DDGS
from ddgs.exceptions import DDGSException, RatelimitException, TimeoutException

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

__version__ = "1.3.0-ddg"

# Engines to aggregate across. All of these support `site:` / `filetype:`
# operators. Google is deliberately excluded (that's the engine that broke
# the original tool and reliably CAPTCHAs scripted queries). "auto" also
# works but can pull in engines like Wikipedia that don't understand these
# operators, so we pin an explicit, curated list instead.
DEFAULT_BACKENDS = "duckduckgo,bing,brave,yahoo,mojeek,startpage"

# Fallback User-Agent pool used with `-u` (randomize) when no user_agents.txt
# is found next to the script. Keeps the tool working out of the box on a
# fresh checkout instead of crashing on a missing file.
DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]

# Retry/backoff settings for transient search failures (rate limits, timeouts).
SEARCH_MAX_RETRIES = 3
SEARCH_BACKOFF_BASE = 5  # seconds; doubles each retry


def ddg_search(query, max_results, pause, backend=DEFAULT_BACKENDS):
    """
    Search across the configured engines for `query` and yield result URLs.

    Mirrors the shape of the old googlesearch.search() generator so the
    rest of the program didn't need to change: caller just iterates URLs.
    Retries with exponential backoff on rate-limit/timeout errors before
    giving up on this filetype's search.
    """
    attempt = 0
    while attempt <= SEARCH_MAX_RETRIES:
        results_seen = 0
        try:
            with DDGS() as ddgs:
                for r in ddgs.text(
                    query,
                    region="wt-wt",
                    safesearch="off",
                    max_results=max_results,
                    backend=backend,
                ):
                    url = r.get("href") or r.get("url") or r.get("link")
                    if url:
                        results_seen += 1
                        yield url
                    if results_seen >= max_results:
                        break
            break  # search completed (even if zero results) -- don't retry

        except (RatelimitException, TimeoutException) as e:
            attempt += 1
            if attempt > SEARCH_MAX_RETRIES:
                print(f"[-] Search backend still rate-limited/timing out after {SEARCH_MAX_RETRIES} retries: {e}")
                break
            backoff = SEARCH_BACKOFF_BASE * (2 ** (attempt - 1))
            print(
                f"[-] Search backend is rate-limiting/timing out (attempt {attempt}/{SEARCH_MAX_RETRIES}). "
                f"Backing off {backoff}s. Consider raising -e too. Details: {e}"
            )
            time.sleep(backoff)
            continue

        except DDGSException as e:
            print(f"[-] Search backend error: {e}")
            break

        except KeyboardInterrupt:
            raise

        except Exception as e:  # noqa: BLE001 - defensive: never let a bad engine crash the whole run
            print(f"[-] Unexpected error during search: {e}")
            break

    # Respect the delay between filetype searches, same as the original tool did
    # between paginated Google requests.
    time.sleep(pause)


class DownloadWorker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            # Grab URL off the queue.
            url = mg.queue.get()

            try:
                headers = {}

                # Assign a User-Agent for each file request.
                if mg.user_agent == "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)":
                    headers["User-Agent"] = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
                elif mg.user_agent is None:
                    user_agent_choice = random.choice(mg.random_user_agents).strip()
                    headers["User-Agent"] = f"{user_agent_choice}"
                else:
                    headers["User-Agent"] = mg.user_agent

                response = requests.get(
                    url,
                    headers=headers,
                    verify=False,
                    timeout=mg.url_timeout,
                    stream=True,
                )

                # Download the file.
                if response.status_code == 200:
                    try:
                        size = int(response.headers["Content-Length"])
                    except KeyError as e:
                        print(
                            f"[-] Exception for url: {url} -- {e} does not exist. Extracting file size from "
                            "response.content length."
                        )
                        size = len(response.content)

                    mg.total_bytes += size

                    # Strip any trailing /'s before extracting file name. Use response.url in case there were
                    # HTTP 301/302 redirects.
                    url_file_name = str(response.url.strip("/").split("/")[-1])

                    # Decode URL file name if it's encoded.
                    filename = urllib.parse.unquote(url_file_name, encoding="utf-8")

                    print(f'[+] Downloading "{filename}" [{size} bytes] from: {response.url}')

                    with open(os.path.join(mg.save_directory, filename), "wb") as fh:
                        for chunk in response.iter_content(chunk_size=1024):
                            if chunk:  # Filter out keep-alive new chunks.
                                fh.write(chunk)
                else:
                    print(f"[-] URL {url} returned HTTP code {response.status_code}")

            except requests.exceptions.RequestException as e:
                print(f"[-] Exception for url: {url} -- {e}")

            mg.queue.task_done()


class Metagoofil:
    """The Metagoofil Class"""

    def __init__(
        self,
        domain,
        delay,
        save_links,
        url_timeout,
        search_max,
        download_file_limit,
        save_directory,
        number_of_threads,
        file_types,
        user_agent,
        download_files,
        backend=DEFAULT_BACKENDS,
    ):
        # Normalize domain input: strip an accidentally-pasted scheme/path/
        # trailing slash so "https://example.com/" and "example.com" behave
        # identically instead of producing a query DDGS silently returns
        # zero results for.
        self.domain = (
            domain.strip()
            .removeprefix("https://")
            .removeprefix("http://")
            .split("/")[0]
        )
        self.delay = delay
        self.save_links = open(save_links, "a") if save_links else None
        self.url_timeout = url_timeout
        self.search_max = search_max
        self.download_file_limit = download_file_limit
        self.save_directory = save_directory

        # Create queue and specify the number of worker threads.
        self.queue = queue.Queue()
        self.number_of_threads = number_of_threads

        self.file_types = file_types

        self.user_agent = user_agent
        # Populate a list of random User-Agents.
        if self.user_agent is None:
            agents_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "user_agents.txt")
            try:
                with open(agents_path) as fp:
                    self.random_user_agents = [line.strip() for line in fp if line.strip()]
                if not self.random_user_agents:
                    raise ValueError("user_agents.txt was empty")
            except (FileNotFoundError, ValueError):
                # No user_agents.txt shipped alongside the script (or it was empty) --
                # fall back to a built-in pool instead of crashing.
                self.random_user_agents = DEFAULT_USER_AGENTS

        self.download_files = download_files
        self.backend = backend
        self.total_bytes = 0
        self.files = []

    def go(self):
        # Kickoff the threadpool.
        for i in range(self.number_of_threads):
            thread = DownloadWorker()
            thread.daemon = True
            thread.start()

        if "ALL" in self.file_types:
            from itertools import product
            from string import ascii_lowercase

            # Generate all three letter combinations.
            self.file_types = ["".join(i) for i in product(ascii_lowercase, repeat=3)]

        for filetype in self.file_types:
            # Stores URLs with files, clear out for each filetype.
            self.files = []

            # Search for the files to download.
            print(
                f"[*] Searching for {self.search_max} .{filetype} files and waiting {self.delay} seconds between searches"
            )
            query = f"filetype:{filetype} site:{self.domain}"

            try:
                for url in ddg_search(query, max_results=self.search_max, pause=self.delay, backend=self.backend):
                    self.files.append(url)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"[-] EXCEPTION: {e}")

            if not self.files:
                print(
                    f"[*] No .{filetype} results for {self.domain}. This can mean the domain genuinely has none "
                    "indexed, or every search engine is currently rate-limiting this IP -- try increasing -e, "
                    "narrowing -b to a single engine, or retrying later."
                )

            # Ensure the file list only contains the requested amount.
            if len(self.files) > self.search_max:
                self.files = self.files[: -(len(self.files) - self.search_max)]

            # Download files if specified with -w switch.
            if self.download_files:
                self.download()
            # Otherwise, just display them.
            else:
                print(f"[*] Results: {len(self.files)} .{filetype} files found")
                for file_name in self.files:
                    print(f"    {file_name}")

            # Save links to output to file.
            if self.save_links:
                for f in self.files:
                    self.save_links.write(f"{f}\n")

        if self.save_links:
            self.save_links.close()

        if self.download_files:
            print(
                "[+] Total download: {} bytes / {:.2f} KB / {:.2f} MB".format(
                    self.total_bytes, self.total_bytes / 1024, self.total_bytes / (1024 * 1024)
                )
            )

    def download(self):
        self.counter = 1
        for url in self.files:
            if self.counter <= self.download_file_limit:
                self.queue.put(url)
                self.counter += 1
        self.queue.join()


def get_timestamp():
    now = time.localtime()
    timestamp = time.strftime("%Y%m%d_%H%M%S", now)
    return timestamp


def csv_list(string):
    return string.split(",")


# http://stackoverflow.com/questions/3853722/python-argparse-how-to-insert-newline-in-the-help-text
class SmartFormatter(argparse.HelpFormatter):
    def _split_lines(self, text, width):
        if text.startswith("R|"):
            return text[2:].splitlines()
        return argparse.HelpFormatter._split_lines(self, text, width)


def positive_int(value):
    try:
        value_int = int(value)
        assert value_int >= 0
        return value_int
    except (AssertionError, ValueError):
        raise argparse.ArgumentTypeError(f"invalid value '{value}', must be an int >= 0")


def positive_float(value):
    try:
        value_float = float(value)
        assert value_float >= 0
        return value_float
    except (AssertionError, ValueError):
        raise argparse.ArgumentTypeError(f"invalid value '{value}', must be a float >= 0")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=f"Metagoofil v{__version__} - Search (via DuckDuckGo) and download specific file types.",
        formatter_class=SmartFormatter,
    )
    parser.add_argument("-d", dest="domain", action="store", required=True, help="Domain to search.")
    parser.add_argument(
        "-b",
        dest="backend",
        action="store",
        default=DEFAULT_BACKENDS,
        help=(
            "R|Comma-separated search engine backend(s) to use, e.g. \"duckduckgo\" or \"bing,brave\".\n"
            f"Default: {DEFAULT_BACKENDS} (queries all, merges results, falls back automatically\n"
            "if one engine is rate-limiting you)."
        ),
    )
    parser.add_argument(
        "-e",
        dest="delay",
        action="store",
        type=positive_float,
        default=5.0,
        help=(
            "Delay (in seconds) between searches. If it's too small DuckDuckGo may rate-limit your IP, too big and "
            "your search may take a while. Default: 5.0"
        ),
    )
    parser.add_argument(
        "-f",
        nargs="?",
        metavar="SAVE_FILE",
        dest="save_links",
        action="store",
        default=False,
        help="R|Save the html links to a file.\n"
        "no -f = Do not save links\n"
        "-f = Save links to html_links_<TIMESTAMP>.txt\n"
        "-f SAVE_FILE = Save links to SAVE_FILE",
    )
    parser.add_argument(
        "-i",
        dest="url_timeout",
        action="store",
        type=positive_int,
        default=15,
        help="Number of seconds to wait before timeout for unreachable/stale pages. Default: 15",
    )
    parser.add_argument(
        "-l",
        dest="search_max",
        action="store",
        type=positive_int,
        default=100,
        help="Maximum results to search. Default: 100",
    )
    parser.add_argument(
        "-n",
        dest="download_file_limit",
        action="store",
        type=positive_int,
        default=100,
        help="Maximum number of files to download per filetype. Default: 100",
    )
    parser.add_argument(
        "-o",
        dest="save_directory",
        action="store",
        default=os.getcwd(),
        help='Directory to save downloaded files. Default is current working directory, "."',
    )
    parser.add_argument(
        "-r",
        dest="number_of_threads",
        action="store",
        type=positive_int,
        default=8,
        help="Number of downloader threads. Default: 8",
    )
    parser.add_argument(
        "-t",
        dest="file_types",
        action="store",
        type=csv_list,
        required=True,
        help=(
            "file_types to download (pdf,doc,xls,ppt,odp,ods,docx,xlsx,pptx). To search all 17,576 three-letter "
            'file extensions, type "ALL"'
        ),
    )
    parser.add_argument(
        "-u",
        dest="user_agent",
        nargs="?",
        default="Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        help="R|User-Agent for file retrieval against -d domain.\n"
        'no -u = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"\n'
        "-u = Randomize User-Agent\n"
        '-u "My custom user agent 2.0" = Your customized User-Agent',
    )
    parser.add_argument(
        "-w",
        dest="download_files",
        action="store_true",
        default=False,
        help="Download the files, instead of just viewing search results.",
    )
    args = parser.parse_args()

    if args.save_directory and args.download_files:
        print(f"[*] Downloaded files will be saved here: {args.save_directory}")
        if not os.path.exists(args.save_directory):
            print(f"[+] Creating folder: {args.save_directory}")
            os.mkdir(args.save_directory)

    if args.save_links is False:
        args.save_links = None
    elif args.save_links is None:
        args.save_links = f"html_links_{get_timestamp()}.txt"

    mg = Metagoofil(**vars(args))
    try:
        mg.go()
        print("[+] Done!")
    except KeyboardInterrupt:
        print("\n[-] Interrupted by user. Cleaning up...")
        if mg.save_links:
            mg.save_links.close()
        sys.exit(130)
