class Site:
    def __init__(
        self, id: str, name: str, latitude: float, longitude: float
    ) -> None: ...
    @property
    def id(self) -> str:
        """The unique identifier of the site."""

    @property
    def name(self) -> str:
        """The name of the site."""

    @property
    def latitude(self) -> float:
        """The latitude coordinate of the site."""

    @property
    def longitude(self) -> float:
        """The longitude coordinate of the site."""

class SiteSetup:
    @property
    def info(self) -> Site | None:
        """The site information."""

    @property
    def kitchens(self) -> list[KitchenSetup]:
        """The list of kitchens in the site."""

class Kitchen:
    @property
    def id(self) -> str:
        """The unique identifier of the kitchen."""

    @property
    def name(self) -> str:
        """The name of the kitchen."""

class KitchenSetup:
    @property
    def info(self) -> Kitchen | None:
        """The kitchen information."""

    @property
    def stations(self) -> list[Station]:
        """The list of stations in the kitchen."""

class Station:
    @property
    def id(self) -> str:
        """The unique identifier of the station."""

    @property
    def name(self) -> str:
        """The name of the station."""

    @property
    def station_type(self) -> str:
        """The type of the station."""

class Ingredient:
    @property
    def id(self) -> str:
        """The unique identifier of the ingredient."""

    @property
    def name(self) -> str:
        """The name of the ingredient."""

    @property
    def description(self) -> str:
        """The description of the ingredient."""

    @property
    def price(self) -> float:
        """The price of the ingredient."""

    @property
    def image_url(self) -> str | None:
        """The image URL of the ingredient."""

class IngredientQuantity:
    @property
    def ingredient_ref(self) -> str:
        """The ingredient reference."""

    @property
    def quantity(self) -> str:
        """The quantity of the ingredient."""

class Instruction:
    @property
    def step(self) -> str:
        """The step name of the instruction."""

    @property
    def description(self) -> str:
        """The description of the instruction."""

    @property
    def required_station(self) -> str:
        """The required station for the instruction."""

    @property
    def expected_duration(self) -> int:
        """The expected duration of the instruction."""

class MenuItem:
    @property
    def id(self) -> str:
        """The unique identifier of the menu item."""

    @property
    def name(self) -> str:
        """The name of the menu item."""

    @property
    def description(self) -> str:
        """The description of the menu item."""

    @property
    def price(self) -> float:
        """The price of the menu item."""

    @property
    def image_url(self) -> str | None:
        """The image URL of the menu item."""

    @property
    def ingredients(self) -> list[IngredientQuantity]:
        """The list of ingredients for the menu item."""

    @property
    def instructions(self) -> list[Instruction]:
        """The list of instructions for the menu item."""

class Brand:
    @property
    def id(self) -> str:
        """The unique identifier of the brand."""

    @property
    def name(self) -> str:
        """The name of the brand."""

    @property
    def description(self) -> str:
        """The description of the brand."""

    @property
    def category(self) -> str:
        """The category of the brand."""

    @property
    def items(self) -> list[MenuItem]:
        """The list of menu items for the brand."""

class SimulationSetup:
    @property
    def sites(self) -> list[SiteSetup]:
        """The list of sites in the simulation."""

    @property
    def brands(self) -> list[str]:
        """The list of brands in the simulation."""

def load_simulation_setup(
    directory: str, options: dict[str, str] | None = None
) -> SimulationSetup:
    """Load a simulation setup from file structure.

    Args:
        directory: fully qualified url for directory containing the simulation setup files.
        options: Optional dictionary of options passed to object store.

    Returns:
        A SimulationSetup object representing the loaded simulation setup.
    """

def run_simulation(
    setup: SimulationSetup,
    duration: int,
    output_location: str,
    routing_location: str,
    dry_run: bool = False,
) -> None:
    """Run a simulation using the provided setup.

    Args:
        setup: The simulation setup to use.
        duration: The duration of the simulation in seconds.
        output_location: The location to save the simulation output.
        routing_location: The location where the routing graph is stored.
        dry_run: Whether to run the simulation in dry run mode.
    """
