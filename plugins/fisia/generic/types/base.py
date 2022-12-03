from dataclasses import asdict, dataclass


@dataclass
class Base:
    def to_dict(self):
        return asdict(self)
