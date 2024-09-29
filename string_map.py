from math import sqrt
from random import randint, seed
from jellyfish import levenshtein_distance
import numpy as np
from sklearn.cluster import AgglomerativeClustering

seed(42)


class StringMap:
    def __init__(self, strings: list[str], dimensions: int) -> None:
        self.strings = np.array(strings)
        self.dimensions = dimensions

        self.pivot_array: list[list[str]] = [[], []]
        self.coords: np.ndarray = np.zeros((len(self.strings), self.dimensions), dtype=float)
    
    def calculate(self):
        for h in range(self.dimensions):
            first_pivot, second_pivot = self.choose_pivot(h)
            self.pivot_array[0].append(first_pivot)
            self.pivot_array[1].append(second_pivot)

            distance = self.get_distance(first_pivot, second_pivot, h)

            if distance == 0:
                self.coords[:, h] = 0
                break
            
            for i in range(len(self.strings)):
                x = self.get_distance(i, first_pivot, h)
                y = self.get_distance(i, second_pivot, h)
                self.coords[i, h] = (x * x + distance * distance - y * y) / (2 * distance)
    
    def choose_pivot(self, h: int) -> tuple[int, int]:
        first_index = randint(0, len(self.strings) - 1)
        second_index = 0

        for _ in range(5):
            second_index = self.get_farthest(first_index, h)
            first_index = self.get_farthest(second_index, h)
        
        return first_index, second_index
                
    def get_distance(self, first_index: int, second_index: int, h: int) -> float | int:
        first, second = self.strings[first_index], self.strings[second_index]
        distance = levenshtein_distance(first, second)

        for i in range(h - 1):
            w = self.coords[first_index][i] - self.coords[second_index][i]
            distance = sqrt(abs(distance * distance - w * w))
        
        return distance

    def get_farthest(self, from_index: str, h: int) -> int:
        return max(range(len(self.strings)), key=lambda x: self.get_distance(from_index, x, h))

    def get_clusters(self) -> np.ndarray:
        clustering = AgglomerativeClustering(n_clusters=None, distance_threshold=1.5)
        return clustering.fit_predict(self.coords)

    def get_centroids(self, clusters: np.ndarray | None = None) -> np.ndarray:
        if clusters is None:
            clusters = self.get_clusters()
        
        return np.array([self.coords[clusters == label].mean(axis=0) for label in np.unique(clusters)])

    def shrink(self, centroids: np.ndarray | None = None, clusters: np.ndarray | None = None) -> None:
        if clusters is None:
            clusters = self.get_clusters()
        if centroids is None:
            centroids = self.get_centroids(clusters=clusters)
        
        self.strings = np.array([self.strings[clusters == label] for label in np.unique(clusters)])
        self.coords = centroids
    
    def add_new_strings(self, strings: list[str]) -> None:
        previous_length = len(self.strings)
        self.strings = np.hstack([self.strings, strings])
        self.coords: np.ndarray = np.vstack([self.coords, np.zeros((len(self.strings), self.dimensions), dtype=float)])

        for h in range(self.dimensions):
            first_pivot, second_pivot = self.pivot_array[0][h], self.pivot_array[1][h]

            distance = self.get_distance(first_pivot, second_pivot, h)
            
            for i in range(previous_length, len(self.strings)):
                x = self.get_distance(i, first_pivot, h)
                y = self.get_distance(i, second_pivot, h)
                self.coords[i, h] = (x * x + distance * distance - y * y) / (2 * distance)
