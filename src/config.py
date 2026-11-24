"""Run-size configuration. All generated data is synthetic."""
from dataclasses import dataclass

@dataclass(frozen=True)
class Scale:
    patients: int
    providers: int
    encounters: int

SCALES = {
    "demo": Scale(patients=1000, providers=100, encounters=10000),
    "medium": Scale(patients=100000, providers=2000, encounters=1000000),
    "large": Scale(patients=1000000, providers=10000, encounters=10000000),
}
