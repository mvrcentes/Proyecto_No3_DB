import time

class File:
	''' 
	Clase para manejar archivos. 
	Un HFile tiene un arreglo de columnas y un arreglo de filas.
	'''
	
	rows = []

	def __init__(self, rows, column_family):
		''' 
		Inicializa el HFile con las filas y la familia de columnas dadas.
		
		@param rows: Lista de objetos Row.
		@param column_family: Nombre de la familia de columnas.
		'''
		self.rows = rows
		self.column_family = column_family

	def get_rows(self):
		''' 
		Devuelve las filas del HFile.
		
		@return: Lista de objetos Row.
		'''
		return self.rows

	def create_row(self, key, column, value, timestamp=None):
		''' 
		Crea una fila con la clave, columna, valor y marca de tiempo dados, y la agrega al HFile.
		
		@param key: Clave de la fila.
		@param column: Nombre de la columna en el formato FamiliaColumna:Columna.
		@param value: Valor a almacenar en la columna.
		@param timestamp: (Opcional) Marca de tiempo de la fila.
		@return: El objeto Row creado.
		'''
		row = Row(key, column, timestamp, value, True)
		self.rows.append(row)
		return row

	def get(self, key, column_family, column, version=1):
		''' 
		Recupera la(s) fila(s) que coincidan con la clave, familia de columnas, columna y versión dadas.
		
		@param key: Clave de la fila.
		@param column_family: Nombre de la familia de columnas.
		@param column: Nombre de la columna.
		@param version: (Opcional) Número de versión a recuperar. Por defecto es 1.
		@return: Lista de objetos Row que coinciden.
		'''
		self.rows.sort(key=lambda x: x.timestamp, reverse=False)
		rows_found = []
		counter = 0
		for row in self.rows:
			col_key = column_family + ":" + column
			if row.key == key and row.column == col_key and row.enabled:
				counter += 1
				if counter == version:
					rows_found.append(row)
					break
		return rows_found

	def delete(self, key, column_family=None, column=None, timestamp=None):
		''' 
		Elimina las filas que coincidan con la clave, familia de columnas, columna y marca de tiempo dadas.
		
		@param key: Clave de la fila.
		@param column_family: (Opcional) Nombre de la familia de columnas. Por defecto es None.
		@param column: (Opcional) Nombre de la columna. Por defecto es None.
		@param timestamp: (Opcional) Marca de tiempo de la fila. Por defecto es None.
		@return: Número de filas eliminadas.
		'''
		rows_deleted = 0
		for row in self.rows:
			if column_family is None and column is None and timestamp is None:
				if row.key == key and row.enabled:
					row.disable()
					rows_deleted += 1
			elif column_family is not None and column is None and timestamp is None:
				if row.key == key and column_family in row.column and row.enabled:
					row.disable()
					rows_deleted += 1
			elif column_family is not None and column is not None and timestamp is None:
				col_key = column_family + ":" + column
				if row.key == key and row.column == col_key and row.enabled:
					row.disable()
					rows_deleted += 1
			elif column_family is not None and column is not None and timestamp is not None:
				col_key = column_family + ":" + column
				if row.key == key and row.column == col_key and row.timestamp == timestamp and row.enabled:
					row.disable()
					rows_deleted += 1
		return rows_deleted

class Row:
	''' 
	Clase para manejar filas. 
	Una fila tiene una clave, nombre de columna, marca de tiempo, valor y un booleano que indica si está habilitada.
	'''

	def __init__(self, key, column, timestamp, value, enabled=True):
		''' 
		Inicializa la fila con la clave, columna, marca de tiempo, valor y estado habilitado dados.
		
		@param key: Clave de la fila.
		@param column: Nombre de la columna en el formato FamiliaColumna:Columna.
		@param timestamp: Marca de tiempo de la fila.
		@param value: Valor a almacenar en la columna.
		@param enabled: (Opcional) Booleano que indica si la fila está habilitada. Por defecto es True.
		'''
		self.key = key
		self.column = column
		self.timestamp = timestamp
		self.value = value
		self.enabled = enabled

	def disable(self):
		''' 
		Deshabilita la fila estableciendo el atributo enabled a False.
		'''
		self.enabled = False