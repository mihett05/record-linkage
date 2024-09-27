from typing import Callable, TypeVar

Input = TypeVar("Input")
Output = TypeVar("Output")


def pipeline(
    *transformers: list[Callable[[Input], Output]],
) -> Callable[[Input], Output]:
    def generated_pipeline(value: Input) -> Output:
        last = value
        for transformer in transformers:
            last = transformer(last)
        return last

    return generated_pipeline
