from file import File as HFile, Row

class Table:
		def __init__(self,name, groups) -> None:
			self.name = name
			self.family_columns = {}
			self.h_file = []
			self.enabled = True
			for fam in groups:
				self.family_columns[fam] = []

		def enable_disable(self):
			if self.enabled == True:
				self.enabled = False
			else:
				self.enabled = True

		def is_enabled(self):
			return self.enabled
		
		def add_family(self,name):
			if not self.enabled:
				return False
			if name in self.family_columns.keys():
				return False
			self.family_columns[name] = []
			return True
		
		def delete_family(self, name):
			if not self.enabled:
				return False
			if name not in self.family_columns.keys():
				return False
			del self.family_columns[name]
			return True
		
		def add_column(self, family, column):
			if not self.enabled:
				return False
			if family not in self.family_columns.keys():
				return False
			if column in self.family_columns[family]:
				return False
			self.family_columns[family].append(column)
			return True
		
		def change_name(self, name):
			if not self.enabled:
				return False
			self.name = name


		# <========================= FUNCIONES DML =========================>
		def put(self, row_key, column_family, column_name, value):
			if not self.enabled:
				return False
			if column_family not in self.family_columns.keys():
				return False
			if column_name not in self.family_columns[column_family]:
				return False
			
			if not self.h_file:
				row_t = Row(row_key, f'{column_family}: {column_name}', value)
				h_file_t = HFile([row_t], column_family)
				self.h_file = [h_file_t]
				return True
			else:
				for hf in self.h_file:
					if column_family == hf.column_family:
						hf.create_row(row_key, f'{column_family}:{column_name}', value)
						return True
					else:
						return False
		
		def get(self, row_key, column_family, column, version=1):
			if not self.enabled:
				return None
			if column_family not in self.family_columns.keys():
				return None
			if column not in self.family_columns[column_family]:
				return None
			for h_file in self.h_file:
				rows = h_file.get(row_key, column_family, column, version)
				if len(rows) != 0:
					return rows
				
		def delete(self, row_key, column_family = None, column = None):
			deleted_rows = 0
			if not self.enabled:
				return deleted_rows
			if column_family != None and column_family not in self.family_columns.keys():
				return deleted_rows
			if column != None and column not in self.family_columns[column_family]:
				return deleted_rows
			for h_file in self.h_file:
				deleted_rows += h_file.delete(row_key, column_family, column)
			return deleted_rows
		
		def truncate(self):
			self.h_file = []
			return True
		
		def count(self):
			count = 0
			for h_file in self.h_file:
				for r in h_file.rows:
					if r.enabled:
						count += 1
			return count
		
		def scan(self, start_row = None, end_row = None, limit = None):
			if not self.enabled:
				return False
			
			rows = []
			if not start_row and not end_row:
				for h_file in self.h_file:
					count = 0
					old_key = None
					for row in h_file.rows:
						if old_key and old_key != row.key:
							count += 1

						if limit and count == limit:
							break

						old_key = row.key
						rows.append(row)

			if start_row and end_row:
				for h_file in self.h_file:
					for row in h_file.rows:
						if row.key >= start_row and row.key < end_row:
							rows.append(row)
			return rows
		

		def describe(self):
			enabled = "ENABLED" if self.enabled else "DISABLED"
			print(f"TABLA: {self.name} esta {enabled}")
			print(self.name)
			print(f"DESCRIPCION DE LA FAMILIA DE COLUMNAS")
			for column_family in self.family_columns.keys():
				print(f"NOMBRE ===> {column_family} VERSIONES ==> 1")
			print(f"{str(len(self.family_columns.keys()))} fila(s)")
