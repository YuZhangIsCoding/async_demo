#!/usr/bin/env python
import click

from .__init__ import __version__


@click.group()
@click.version_option(version=__version__)
def main():
    pass


if __name__ == "__main__":
    main()
