from infrasys.models import InfraSysBaseModel


class IDManager(InfraSysBaseModel):
    """Manages IDs for components and time series."""

    next_id: int

    def get_next_id(self) -> int:
        """Return the next available ID."""
        next_id = self.next_id
        self.next_id += 1
        return next_id
