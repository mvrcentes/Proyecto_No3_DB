import pandas as pd
import numpy as np

# Definimos las listas de datos ficticios para generar la base de datos
titulos = ["Inception", "The Matrix", "The Godfather", "Parasite", "Interstellar", 
           "La La Land", "Spirited Away", "The Dark Knight", "Pulp Fiction", "Schindler's List", 
           "Avengers: Endgame", "The Lion King", "Fight Club", "Forrest Gump", "Gladiator", 
           "Titanic", "Jurassic Park", "Star Wars", "The Shawshank Redemption", "The Lord of the Rings"]

generos = ["Ciencia Ficción", "Drama", "Thriller", "Musical", "Animación", "Acción", "Crimen", "Historia", "Comedia", "Romance"]

directores = ["Christopher Nolan", "Wachowski Sisters", "Francis Ford Coppola", "Bong Joon-ho", 
              "Damien Chazelle", "Hayao Miyazaki", "Quentin Tarantino", "Steven Spielberg", 
              "James Cameron", "Ridley Scott", "Martin Scorsese", "Peter Jackson", "George Lucas", "David Fincher"]

paises = ["USA", "South Korea", "Japan", "UK", "France", "Germany", "Italy", "Spain", "Australia", "Canada"]

# Generamos datos aleatorios para 10,000 películas
num_peliculas = 10000
data = {
    "id": range(1, num_peliculas + 1),
    "título": np.random.choice(titulos, num_peliculas),
    "año": np.random.randint(1950, 2024, num_peliculas),
    "género": np.random.choice(generos, num_peliculas),
    "director": np.random.choice(directores, num_peliculas),
    "duración": np.random.randint(80, 180, num_peliculas),
    "calificación": np.round(np.random.uniform(1.0, 10.0, num_peliculas), 1),
    "país": np.random.choice(paises, num_peliculas)
}

# Creamos un DataFrame y lo guardamos en un archivo CSV
df_peliculas = pd.DataFrame(data)
file_path = "peliculas.csv"
df_peliculas.to_csv(file_path, index=False)

file_path