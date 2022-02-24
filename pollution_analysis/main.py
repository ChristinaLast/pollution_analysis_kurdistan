import click

from config.model_settings import FlaringScraperConfig, FlaringLoaderConfig, FlaringDescriptorConfig
from src.flaring_scraper import FlaringScraper
from src.flaring_descriptor import FlaringDescriptor
from src.flaring_loader import FlaringLoader


class FlaringScraperFlow:
    def __init__(self, scraper_config):
        self.scraper_config = FlaringScraperConfig()

    def execute(self):
        # Trigger the authentication flow.
        flaring_scraper = FlaringScraper.from_dataclass_config(self.scraper_config)

        flaring_scraper.execute()


class FlaringLoaderFlow:
    def __init__(self):
        self.flaring_loader_config = FlaringLoaderConfig()

    def execute(self):
        # Trigger the authentication flow.
        flaring_loader = FlaringLoader.from_dataclass_config(self.flaring_loader_config)

        flaring_loader.execute()


class FlaringDescriptorFlow:
    def __init__(self):
        self.flaring_descriptor_config = FlaringDescriptorConfig()

    def execute(self):
        # Trigger the authentication flow.
        flaring_descriptor = FlaringDescriptor.from_dataclass_config(self.flaring_descriptor_config)

        flaring_descriptor.execute()


@click.command("flaring_loader", help="Load flaring data from local folder")
def load_flaring_data():
    FlaringLoaderFlow().execute()

@click.command("flaring_descriptor", help="Describe temporal changes in flaring in a coutry's preprocessed data")
def describe_flaring_data():
    FlaringDescriptorFlow().execute()


@click.group(
    "flaring-pollution-analyisis",
    help="Library aiming to analyise the level and impact of flaring in the Kurdistan region of Iraq",
)
@click.pass_context
def cli(ctx):
    ...


cli.add_command(load_flaring_data)
cli.add_command(describe_flaring_data)

if __name__ == "__main__":
    cli()
