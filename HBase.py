from tabla import Tabla
from file import File as HFile, Row
import numpy as np
import csv


class HBase:
	def __init__(self) -> None:
		"""
		Constructor para inicializar HBase con un diccionario vacío.
		Este diccionario simulará las tablas, facilitando su manejo.
		"""
		self.tables = {}  # Inicializa el diccionario vacío que contendrá las tablas

	def cargar_data(self):
		data = []
		with open('peliculas.csv', 'r', encoding='utf-8') as csvfile:
			reader = csv.reader(csvfile)
			for row in reader:
				if len(row) == 8:
					data.append(row)
		data = np.array(data)
		data = data[~np.all(data == '', axis=1)]
		data = data[1:]
		
		self.tables['data'] = Tabla('movies', ['movie_details', 'rating'])
		self.tables['data'].add_column('movie_details', 'id')
		self.tables['data'].add_column('movie_details', 'title')
		self.tables['data'].add_column('movie_details', 'year')
		self.tables['data'].add_column('movie_details', 'genre')
		self.tables['data'].add_column('movie_details', 'duration')
		self.tables['data'].add_column('movie_details', 'country')

		self.tables['data'].add_column('rating', 'id')
		self.tables['data'].add_column('rating', 'title')
		self.tables['data'].add_column('rating', 'director')
		self.tables['data'].add_column('rating', 'rating')

		count_movie_details = 1
		for row in data:
			self.Put('data', str(count_movie_details), 'movie_details', 'id', row[0])
			self.Put('data', str(count_movie_details), 'movie_details', 'title', row[1])
			self.Put('data', str(count_movie_details), 'movie_details', 'year', row[2])
			self.Put('data', str(count_movie_details), 'movie_details', 'genre', row[3])
			self.Put('data', str(count_movie_details), 'movie_details', 'duration', row[5])
			self.Put('data', str(count_movie_details), 'movie_details', 'country', row[7])
			count_movie_details += 1

		count_rating = 1
		for row in data:
			self.Put('data', str(count_rating), 'rating', 'id', row[0])
			self.Put('data', str(count_rating), 'rating', 'title', row[2])
			self.Put('data', str(count_rating), 'rating', 'director', row[4])
			self.Put('data', str(count_rating), 'rating', 'rating', row[6])
			count_rating += 1
		
		# print('Data Cargada Exitosamente!!!')
	# id,título,año,género,director,duración,calificación,país


	def Create_Test_Table(self):
		self.tables['test'] = Tabla('users', ['general', 'address'])
		self.tables['test'].add_column('general', 'name')
		self.tables['test'].add_column('general', 'age')
		self.tables['test'].add_column('address', 'street')
		self.tables['test'].add_column('address', 'city')
		hfile = HFile([
		Row('1', 'general:name', '1', 'John'),
		Row('1', 'general:age', '1', 20),
		], 'general')
		# For address info
		hfile2 = HFile([
				Row('1', 'address:street', '1', '123 Main St'),
				Row('1', 'address:city', '1', 'New York'),
		], 'address')
		self.tables['test'].h_files.append(hfile)
		self.tables['test'].h_files.append(hfile2)

			
	def Create_Test_Table2(self):
		self.tables['test'] = Tabla('users', ['general', 'address'])
		self.tables['test'].add_column('general', 'name')
		self.tables['test'].add_column('general', 'age')
		self.tables['test'].add_column('address', 'street')
		self.tables['test'].add_column('address', 'city')

		hfile = HFile([
			Row(1, 'general:name', 1, 'John'),
			Row(1, 'general:age', 1, 20),
			Row(2, 'general:name', 1, 'Joshua'),
			Row(2, 'general:age', 1, 25),
			Row(3, 'general:name', 1, 'Sofia'),
			Row(3, 'general:age', 1, 22),
			Row(4, 'general:name', 1, 'Rose'),
			Row(4, 'general:age', 1, 23),
			Row(5, 'general:name', 1, 'Lily'),
			Row(5, 'general:age', 1, 24),
		], 'general')
		# For address info
		hfile2 = HFile([
			Row(1, 'address:street', 1, '123 Main St'),
			Row(1, 'address:city', 1, 'New York'),
			Row(2, 'address:street', 1, '456 Main St'),
			Row(2, 'address:city', 1, 'San Francisco'),
			Row(3, 'address:street', 1, '789 Main St'),
			Row(3, 'address:city', 1, 'Miami'),
			Row(4, 'address:street', 1, '101 Main St'),
			Row(4, 'address:city', 1, 'Los Angeles'),
			Row(5, 'address:street', 1, '102 Main St'),
			Row(5, 'address:city', 1, 'Chicago'),
		], 'address')
		self.tables['test'].h_files.append(hfile)
		self.tables['test'].h_files.append(hfile2)
	
	def Create(self, name, familia_columnas):
		"""
		Crea una nueva tabla con el nombre especificado y las familias de columnas dadas.

		Args:
			name (str): El nombre de la tabla a crear.
			familia_columnas (list): Una lista de nombres de las familias de columnas.

		Returns:
			bool: True si la tabla se crea correctamente, False si la tabla ya existe.
		"""
		if name not in self.tables.keys():  # Verifica si la tabla no existe
			table = Tabla(name, familia_columnas)  # Crea una instancia de la clase Tabla
			self.tables[name] = table  # Añade la tabla al diccionario de tablas
			return True  # Devuelve True si la tabla se ha creado correctamente
		return False  # Devuelve False si la tabla ya existe

	def List(self):
		"""
		Lista los nombres de todas las tablas existentes.

		Returns:
			dict_keys: Una vista de los nombres de las tablas.
		"""
		return self.tables.keys()  # Devuelve las claves del diccionario de tablas

	def Enable(self, name):
		"""
		Habilita la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a habilitar.
		
		Retorna:
		bool: True si la tabla fue habilitada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			self.tables[name].enable()  # Habilita la tabla
			return True  # Retorna True indicando éxito
		return False  # Retorna False si la tabla no existe

	def Disable(self, name):
		"""
		Deshabilita la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a deshabilitar.
		
		Retorna:
		bool: True si la tabla fue deshabilitada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			self.tables[name].disable()  # Deshabilita la tabla
			return True  # Retorna True indicando éxito
		return False  # Retorna False si la tabla no existe
	
	def Is_enable(self, name):
		"""
		Verifica si la tabla con el nombre dado está habilitada.
		
		Parámetros:
		name (str): El nombre de la tabla a verificar.
		
		Retorna:
		bool: True si la tabla está habilitada, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			return self.tables[name].is_enable()  # Retorna el estado de habilitación
		return False  # Retorna False si la tabla no existe
	
	def Alter_table_add(self, name, familia_columnas):
		"""
		Agrega una familia de columnas a la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a modificar.
		familia_columnas (str): El nombre de la familia de columnas a agregar.
		
		Retorna:
		bool: True si la familia de columnas fue agregada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			return self.tables[name].add_family(familia_columnas)  # Agrega la familia de columnas
		return False  # Retorna False si la tabla no existe
	
	def Alter_table_delete(self, name, familia_columnas):
		"""
		Elimina una familia de columnas de la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a modificar.
		familia_columnas (str): El nombre de la familia de columnas a eliminar.
		
		Retorna:
		bool: True si la familia de columnas fue eliminada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			return self.tables[name].delete_family(familia_columnas)  # Elimina la familia de columnas
		return False  # Retorna False si la tabla no existe
		
	def Alter_table_add_column(self, name, familia_columnas, columna):
		"""
		Agrega una columna a una familia de columnas especificada en la tabla con el nombre dado si existe.
		
		Parámetros:
		name (str): El nombre de la tabla a modificar.
		familia_columnas (str): El nombre de la familia de columnas a modificar.
		columna (str): El nombre de la columna a agregar.
		
		Retorna:
		bool: True si la columna fue agregada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys():  # Verifica si la tabla existe
			return self.tables[name].add_column(familia_columnas, columna)  # Agrega la columna
		return False  # Retorna False si la tabla no existe
	
	def Alter_table_name(self, table, nuevo_nombre):
		"""
		Renombra una tabla si existe y el nuevo nombre no está ya en uso.
		
		Parámetros:
		table (str): El nombre actual de la tabla a renombrar.
		nuevo_nombre (str): El nuevo nombre para la tabla.
		
		Retorna:
		bool: True si la tabla fue renombrada exitosamente, False en caso contrario.
		"""
		nombre_viejo = table in self.tables.keys()  # Verifica si la tabla actual existe
		nombre_repetido = nuevo_nombre in self.tables.keys()  # Verifica si el nuevo nombre ya está en uso

		if nombre_viejo and not nombre_repetido:
			tabla_anterior = self.tables[table]  # Obtiene la tabla actual
			del self.tables[table]  # Elimina la entrada de la tabla actual
			tabla_anterior.change_name(nuevo_nombre)  # Cambia el nombre de la tabla
			self.tables[nuevo_nombre] = tabla_anterior  # Añade la tabla con el nuevo nombre
			return True  # Retorna True indicando éxito
		return False  # Retorna False si el renombrado no es posible
	
	def Delete_table(self, name):
		"""
		Elimina la tabla con el nombre dado si existe y está deshabilitada.
		
		Parámetros:
		name (str): El nombre de la tabla a eliminar.
		
		Retorna:
		bool: True si la tabla fue eliminada exitosamente, False en caso contrario.
		"""
		if name in self.tables.keys() and not self.tables[name].is_enable():  # Verifica si la tabla existe y está deshabilitada
			del self.tables[name]  # Elimina la tabla
			return True  # Retorna True indicando éxito
		return False  # Retorna False si la tabla no existe o está habilitada

	def Delete_all_table(self):
		"""
		Elimina todas las tablas.
		
		Retorna:
		bool: True indicando que todas las tablas fueron eliminadas.
		"""
		self.tables = {}  # Elimina todas las tablas
		return True  # Retorna True indicando éxito

	def Describe(self, nombre_tabla):
		"""
		Describe la tabla con el nombre dado si existe.
		
		Parámetros:
		nombre_tabla (str): El nombre de la tabla a describir.
		
		Retorna:
		bool: True si la descripción se realizó exitosamente, False en caso contrario.
		"""
		if nombre_tabla not in self.tables.keys():  # Verifica si la tabla no existe
			return False  # Retorna False si la tabla no existe
		self.tables[nombre_tabla].describe()  # Describe la tabla
		return True  # Retorna True indicando éxito

	def Get(self, table_name, row_key, columns, version=1):
		"""
		Obtiene los datos de una tabla específica y una fila dada.

		@param table_name: Nombre de la tabla.
		@param row_key: Clave de la fila.
		@param columns: Lista de columnas en formato "cf:col" (familia de columnas:columna).
		@param version: Versión de los datos a obtener (por defecto es 1).
		@return: Lista de filas encontradas o None si la tabla no existe.
		"""
		if table_name in self.tables.keys():
			rows = []
			for column in columns:
				cf, col = column.split(":")
				rows_found = self.tables[table_name].get(row_key, cf, col, version)
				if rows_found is not None:
					# Append rows_found list contents to rows
					rows.extend(rows_found)
			return rows
		return None

	def Delete(self, table_name, row_key=None, column_family=None, column_name=None, timestamp=None):
		"""
		Elimina datos de una tabla específica.

		@param table_name: Nombre de la tabla.
		@param row_key: Clave de la fila (opcional).
		@param column_family: Familia de columnas (opcional).
		@param column_name: Nombre de la columna (opcional).
		@param timestamp: Timestamp de la eliminación (opcional).
		@return: Resultado de la operación de eliminación o 0 si la tabla no existe.
		"""
		if table_name in self.tables.keys():
			return self.tables[table_name].delete(row_key, column_family, column_name, timestamp)
		return 0

	
	def Scan(self, table_name, row_start=None, row_stop=None, limit=None):
		"""
		Escanea una tabla en un rango de filas.

		@param table_name: Nombre de la tabla.
		@param row_start: Clave de la fila de inicio (opcional).
		@param row_stop: Clave de la fila de fin (opcional).
		@param limit: Límite de filas a escanear (opcional).
		@return: Resultados del escaneo o None si la tabla no existe.
		"""
		if table_name in self.tables.keys():
			return self.tables[table_name].scan(row_start, row_stop, limit)
		return None

	def Count(self, nombre_tabla):
		"""
		Cuenta el número de filas en una tabla.

		@param nombre_tabla: Nombre de la tabla.
		@return: Número de filas o False si la tabla no existe.
		"""
		if nombre_tabla in self.tables.keys():
			return self.tables[nombre_tabla].count()
		return False

	def Put(self, table_name, row_key, column_family, column_name, value, timestamp=None):
		"""
		Inserta un dato en una tabla.

		@param table_name: Nombre de la tabla.
		@param row_key: Clave de la fila.
		@param column_family: Familia de columnas.
		@param column_name: Nombre de la columna.
		@param value: Valor a insertar.
		@param timestamp: Timestamp de la inserción (opcional).
		@return: Resultado de la operación de inserción o False si la tabla no existe.
		"""
		if table_name in self.tables.keys():
			return self.tables[table_name].put(table_name, row_key, column_family, column_name, value, timestamp)
		return False

	def Truncate(self, nombre_tabla):
		"""
		Trunca una tabla (elimina todos los datos).

		@param nombre_tabla: Nombre de la tabla.
		@return: True si la operación es exitosa, False si la tabla no existe.
		"""
		if nombre_tabla in self.tables.keys():
			self.Disable(nombre_tabla)
			self.tables[nombre_tabla].truncate()
			self.Enable(nombre_tabla)
			return True
		return False