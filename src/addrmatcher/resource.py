from urllib import request
import re
import json
import os
import sys
from colorama import Fore, Style
import argparse
import signal

CWD = os.path.abspath(os.getcwd())

color_code = {
    "default": "",
    "red": Fore.RED,
    "green": Style.BRIGHT + Fore.GREEN,
}
erase_line = "\x1b[2K"


def print_text(
    text, color="default", in_place=False, **kwargs
):  # type: (str, str, bool, any) -> None
    """
    print text to console
    :param text string: text to print
    :param color string: can be one of "red" or "green", or "default"
    :param in_place boolean: whether to erase previous line and print in place
    :param kwargs: other keywords passed to built-in print
    """
    if in_place:
        print("\r" + erase_line, end="")
    print(color_code[color] + text + Style.RESET_ALL, **kwargs)


def create_url(url):
    """
    produce a URL that is compatible with Github's REST API from the input url
    This can handle blob or tree paths.

    :param url string: Url to the data directory in Github repository
    """
    repo_only_url = re.compile(
        r"https:\/\/github\.com\/[a-z\d](?:[a-z\d]|-(?=[a-z\d])){0,38}\/[a-zA-Z0-9]+$"
    )
    re_branch = re.compile("/(tree|blob)/(.+?)/")

    # Check if the given url is a url to a GitHub repo.
    # If it is, inform the user to use 'git clone' to download it
    if re.match(repo_only_url, url):
        print_text(
            "✘ The given url is a complete repository. "
            "Use 'git clone' to download the repository",
            "red",
            in_place=True,
        )
        sys.exit()

    # Extract the branch name from the given url (e.g master)
    branch = re_branch.search(url)
    download_dirs = url[branch.end():]
    api_url = (
        url[: branch.start()].replace("github.com", "api.github.com/repos", 1)
        + "/contents/"
        + download_dirs
        + "?ref="
        + branch.group(2)
    )
    return api_url, download_dirs


def download_data(country="Australia", output_dir=CWD):
    """
    Downloads the files and directories and sub-directories in repo_url.
    param country string: country name which will be
    sub-directory name example - data/Australia/
    """

    # This is the temporary place to host of data files.
    # Example - data/Australia/*.parquet
    repo_url = (
        "https://github.com/uts-mdsi-ilab2-synergy/addrmatcher/tree/main/data/"
        + country
        + "/"
    )

    # generate the url which returns the JSON data
    api_url, download_dirs = create_url(repo_url)

    # To handle file names.
    if len(download_dirs.split(".")) == 0:
        dir_out = os.path.join(output_dir, download_dirs)
    else:
        print("length ", len(download_dirs.split(".")))
        dir_out = os.path.join(output_dir,
                               "/".join(download_dirs.split("/")[:-1]))

    # make a directory with the name which is taken from the actual repo
    os.makedirs(dir_out, exist_ok=True)

    try:
        opener = request.build_opener()
        opener.addheaders = [("User-agent", "Mozilla/5.0")]
        request.install_opener(opener)
        response = request.urlretrieve(api_url)
    except KeyboardInterrupt:
        # when CTRL+C is pressed during the execution of this script,
        # bring the cursor to the beginning,
        # erase the current line, and dont make a new line
        print_text("✘ Got interrupted", "red", in_place=True)
        sys.exit()

    # total files count
    total_files = 0

    # Writing into file
    with open(response[0], "r") as f:
        data = json.load(f)
        # getting the total number of files so that we
        # can use it for the output information later
        total_files += len(data)

        # If the data is a file, download it as one.
        if isinstance(data, dict) and data["type"] == "file":
            try:
                # download the file
                opener = request.build_opener()
                opener.addheaders = [("User-agent", "Mozilla/5.0")]
                request.install_opener(opener)
                request.urlretrieve(
                    data["download_url"], os.path.join(dir_out, data["name"])
                )
                # bring the cursor to the beginning,
                # erase the current line, and dont make a new line
                print_text(
                    "Downloaded====:" + Fore.WHITE + "{}".format(data["name"]),
                    "green",
                    in_place=True,
                )
                return total_files

            except KeyboardInterrupt:
                # when CTRL+C is pressed during the execution of this script,
                # bring the cursor to the beginning,
                # erase the current line, and dont make a new line
                print_text("✘ Got interrupted", "red", in_place=False)
                sys.exit()

        for file in data:
            file_url = file["download_url"]
            file_name = file["name"]

            path = file["path"]
            dirname = os.path.dirname(path)
            print("path", path)

            if dirname != "":
                os.makedirs(os.path.dirname(path), exist_ok=True)
            else:
                pass

            if file_url is not None:
                try:
                    opener = request.build_opener()
                    opener.addheaders = [("User-agent", "Mozilla/5.0")]
                    request.install_opener(opener)
                    # download the file
                    request.urlretrieve(file_url, path)

                    # bring the cursor to the beginning,
                    # erase the current line, and dont make a new line
                    print_text(
                        "Downloaded" + Fore.WHITE + "{}".format(file_name),
                        "green",
                        in_place=False,
                        end="\n",
                        flush=True,
                    )

                except KeyboardInterrupt:
                    # when CTRL+C is pressed during
                    # the execution of this script,
                    # bring the cursor to the beginning,
                    # erase the current line, and dont make a new line
                    print_text("✘ Got interrupted", "red", in_place=False)
                    sys.exit()
            else:
                download_data(file["html_url"], dir_out)

    return total_files


def download():
    """
    Trigger the download_data function and read the argument from user's CLI
    """

    if sys.platform != "win32":
        # disbale CTRL+Z
        signal.signal(signal.SIGTSTP, signal.SIG_IGN)

    parser = argparse.ArgumentParser(
        description="Download directories/folders from GitHub"
    )

    parser.add_argument(
        "--country",
        "-cty",
        default="Australia",
        help="The country of data to which the matching "
        "will apply to. (Default is Australia if not specified)",
    )

    args = parser.parse_args()
    country = args.country

    download_data(country)
    print_text("✔ Download complete", "green", in_place=True)


if __name__ == "__main__":
    download()
