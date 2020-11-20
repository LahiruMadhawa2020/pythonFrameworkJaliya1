from abc import ABC, abstractmethod


class FlatteningFiles(ABC):

    # abstract method
    @abstractmethod
    def convert_to_pdf(self, **kwargs):
        pass

