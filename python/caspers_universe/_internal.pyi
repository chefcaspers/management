class Site:
    def __init__(self, name: str, latitude: float, longitude: float) -> None: ...
    @property
    def name(self) -> str:
        """The name of the site."""

    @property
    def latitude(self) -> float:
        """The latitude coordinate of the site."""

    @property
    def longitude(self) -> float:
        """The longitude coordinate of the site."""
