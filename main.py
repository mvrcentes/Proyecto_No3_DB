from HBase import HBase
import time
import os

def clear_screen():
    # Verifica el sistema operativo y ejecuta el comando adecuado
    if os.name == 'posix':  # Linux y macOS
        os.system('clear')


def main(hbase):
	"""
	Función principal que actúa como simulador de comandos para HBase.

	Crea una instancia de HBase y permite la entrada de comandos en un bucle continuo.
	Los comandos admitidos permiten la creación de tablas, listado de tablas, y otras operaciones DDL y DML.
	"""
	
	while True:  # Bucle continuo para recibir comandos
		comando = input(">>")  # Espera por la entrada del comando
		if comando == "exit":  # Si el comando es "exit", termina el simulador
			break
		comando  = comando.split(" ")  # Divide el comando en palabras
		while "" in comando:  # Remueve entradas vacías del comando
			comando.remove("")
		if comando[0] == "create":  # Si el comando es "create"
			contenido = "".join(comando[1:])  # Junta los elementos del comando
			contenido = contenido.split(",")  # Divide por comas para obtener nombre y familias de columnas
			nombre_tabla = contenido[0][1:-1]  # Obtiene el nombre de la tabla
			tabla_columns_families = contenido[1:]  # Obtiene las familias de columnas
			for i in range(0, len(tabla_columns_families)):
				tabla_columns_families[i] = tabla_columns_families[i][1:-1]  # Limpia las cadenas de texto
			if not hbase.Create(nombre_tabla, tabla_columns_families):  # Intenta crear la tabla
				print(f">> La tabla <{nombre_tabla}> no existe")
			else:
				print(f">> La tabla <{nombre_tabla}> se ha creado")

		elif comando[0] == "list":  # Si el comando es "list"
			print(f"TABLA(s)")
			for name in hbase.List():  # Lista los nombres de las tablas
				print(f">> {str(name)}")  # Imprime cada nombre de tabla

		elif comando[0] == "disable":
			nombre_tabla = comando[1][1:-1]  # Obtiene el nombre de la tabla, eliminando las comillas
			if not hbase.Disable(nombre_tabla):  # Intenta deshabilitar la tabla
				print(f">> La tabla <{nombre_tabla}> ya existe.")  # Imprime un mensaje si la tabla ya existe
			else:
				print(f">> La tabla {nombre_tabla} se ha deshabilitado.")  # Imprime un mensaje si la tabla se ha deshabilitado

		elif comando[0] == "enable":
			nombre_tabla = comando[1][1:-1]  # Obtiene el nombre de la tabla, eliminando las comillas
			if not hbase.Enable(nombre_tabla):  # Intenta habilitar la tabla
				print(f">> La tabla <{nombre_tabla}> no existe.")
			else:
				print(f">> La tabla <{nombre_tabla}> se ha habilitado.")

		elif comando[0] == "is_enabled":
			nombre_tabla = comando[1][1:-1] # Obteine el nombre de la tabla, eliminando las comillas
			print(f">> {str(hbase.Is_enable(nombre_tabla))}") # muestra el estado del elemento (true -> enable, false -> disable)

		elif comando[0] == "alter":
			nombre_tabla = comando[1][1:-2]
			contenido = " ".join(comando[2:])
			claves = [-1,-1]
			for i in range(0, len(contenido)):
				if contenido[i] == "{":
					claves[0] = i
				elif contenido[i] == "}":
					claves[1] = i
			contenido = contenido[claves[0]+1:claves[1]]
			contenido = contenido.split(" => ")
			if len(contenido) != 2:
				print(f">> Error con el comando.")
			elif contenido[0].upper() == "NAME":
				if hbase.Alter_table_name(nombre_tabla, contenido[1][1:-1]):
					print(f">> Se ha modificado la tabla <{nombre_tabla}> a <{contenido[1][1:-1]}>")
				else:
					print(f">> Ha ocurrido un error en la modificacion, nombre repetido")
			elif contenido[0].upper() == "ADD":
				if ":" not in contenido[1][1:-1]:
					if hbase.Alter_table_add(nombre_tabla, contenido[1][1:-1]):
						print(f">> Se ha agregado la familia <{contenido[1][1:-1]}> a la tabla <{nombre_tabla}>")
					else:
						print(f">> Ha ocurrido un error, tabla inexistente.")
				else:
					familia_columnas, columna = contenido[1][1:-1].split(":")
					if hbase.Alter_table_add_column(nombre_tabla, familia_columnas, columna):
						print(f">> Se ha agregado la columna <{columna}> a la familia <{familia_columnas}> de la tabla <{nombre_tabla}>")
					else:
						print(f">> Ha ocurrido un error, tabla inexistente.")
			elif contenido[0] == "DELETE":
				if hbase.Alter_table_delete(nombre_tabla, contenido[1][1:-1]):
					print(f">> Se ha eliminado la familia <{contenido[1][1:-1]}> a la tabla <{nombre_tabla}>")
				else:
					print(f">> Ha ocurrido un error, tabla inexistente")

		elif comando[0] == "drop":
			nombre_tabla = comando[1][1:-1]
			if hbase.Delete_table(nombre_tabla):
				print(f">> Se ha eliminado la tabla <{nombre_tabla}>")
			else:
				print(f"Ha ocurrido un error, tabla inexistente o tabla habilitada, deshabilitar en caso habilitada")
		
		elif comando[0] == "drop_all":
			if hbase.Delete_all_table():
				print(f">> Se han eliminado todas las tablas")
			else:
				print(f">> Algo salió mal.")

		elif comando[0] == "describe":
			contenido = comando[1][1:-1]
			res = hbase.Describe(contenido)
			if not res:
				print(f">> Error al realizar el describe de la tabla <{comando[1]}>, No existe")

		# <========================== FUNCIONES DML  ==========================>
		elif comando[0] == "put":
			comando[1].replace("'","")
			arguments = comando[1].split(",")
			if len(arguments) < 4:
					print(">> Error con el comando")
					continue
			table_name = arguments[0][1:-1]
			row = arguments[1]
			column_family = arguments[2][1:-1].split(':')[0]
			column_name = arguments[2][1:-1].split(':')[1]
			value = arguments[3][1:-1]
			timestamp = arguments[4] if len(arguments) == 5 else None

			if hbase.Put(table_name, row, column_family, column_name, value, timestamp):
				print(f">> Se ha insertado el registro en la tabla <{table_name}>")
			else:
				print(">> Ha ocurrido un error")

		elif comando[0] == "get":
			comando[1].replace("'","")
			content = "".join(comando[1:])
			content = content.split(",")
			table_name = content[0].replace("'","")
			if len(content) < 3:
				print(">> Error con el comando")
				continue
			
			row_key = content[1].replace("'","")
			extra = ",".join(content[2:])
			extra = extra.replace("'","")
			extra = extra.replace(" ","")
			extra = extra.replace("{","")
			extra = extra.replace("}","")
			parameters = extra.split(",")
			column_family = None
			column = None
			version = 1
			for p in parameters:
				key, value = p.split("=>")
				if key == "COLUMN":
					column_family, column = value.split(":")
				if key == "VERSION":
					version = int(value)
			
			rows = hbase.Get(table_name, row_key,[column_family + ":" + column], version)
			if rows is None or len(rows) == 0:
				print(">> No se encontraron registros")
			else:
				for row in rows:
					print(">> Key:" + str(row.key))
					print(">> Value:" + str(row.value))
					print(">> Timestamp:" + str(row.timestamp))

		elif comando[0] == "scan":
			comando[1].replace("'","")
			arguments = comando[1].split(",")
			if len(arguments) < 1:
					print(">> Error con el comando")
					continue
			table_name = arguments[0][1:-1]

			lenn = len(arguments)
			start, end, limit = None, None, None
			if len(arguments) == 3:
				if arguments[1][0] != '{' and arguments[2][-1] != '}':
					print(">> Error con el comando")
					continue
				else:
					arg1 = arguments[1][1:]
					arg2 = arguments[2][:-1]

					if '=>' not in arg1 or '=>' not in arg2:
						print(">> Error con el comando")
						continue
					else:
						start = arg1.split('=>')[1]
						end = arg2.split('=>')[1]
						start = int(start)
						end = int(end)
						keyword_start = arg1.split('=>')[0]
						keyword_end = arg2.split('=>')[0]
						if keyword_start != 'STARTROW' or keyword_end != 'ENDROW':
							print(">> Error con el comando")
							continue

			if len(arguments) == 2:
				if arguments[1][0] != '{' and arguments[1][-1] != '}':
					print(">> Error con el comando")
					continue
				else:
					arg1 = arguments[1][1:-1]
					if '=>' not in arg1:
						print(">> Error con el comando")
						continue
					else:
						limit = arg1.split('=>')[1]
						limit = int(limit)
						keyword_lim = arg1.split('=>')[0]
						if keyword_lim != 'LIMIT':
							print(">> Error con el comando")
							continue	

			if table_name not in hbase.tables.keys():
				print(">> La tabla '" + table_name + "' no existe")
				continue
			else:
				if start and end:
					if start > end:
						print(">> Error con el comando, rangos no validos")
						continue
					elif start == end:
						print(">> Error con el comando, STARTROW y ENDROW no pueden ser iguales")
						continue
					elif not hbase.Scan(table_name=table_name, row_start=start, row_stop=end):
						print(">> La tabla '" + table_name + "' no tiene registros en ese rango")
					else:
						for row in hbase.Scan(table_name=table_name, row_start=start, row_stop=end):
							if row.enabled:
								print(" Key:" + str(row.key) + " value:" + str(row.value) + " timestamp:" + str(row.timestamp))
				else:
					if limit:
						if not hbase.Scan(table_name=table_name, limit=limit):
							print(">> La tabla '" + table_name + "' no tiene registros con las especificaciones dadas 1")
						else:
							for row in hbase.Scan(table_name=table_name, limit=limit):
								if row.enabled:
									print(" Key:" + str(row.key) + " value:" + str(row.value) + " timestamp:" + str(row.timestamp))
					else:
						if not hbase.Scan(table_name):
							print(">> La tabla '" + table_name + "' no tiene registros con las especificaciones dadas 2")
						else:
							for row in hbase.Scan(table_name):
								if row.enabled:
									print(" Key:" + str(row.key) + " value:" + str(row.value) + " timestamp:" + str(row.timestamp))


		elif comando[0] == "delete":
			comando[1].replace("'","")
			multiple_keys = False
			keys_found = ''
			for c in comando[1]:		
				if multiple_keys and c != '}':
					keys_found += c
				if c == "{":
					multiple_keys = True

			content = "".join(comando[1:])
			content = content.split(",")
			table_name = content[0]
			table_name = table_name.replace("'","")
			row_key = None
			column_identifier = None
			timestamp = None
			if len(content) < 2:
				print(">> Error con el comando")
				continue
			row_key = content[1]
			column_name = None
			column_family = None
			if len(content) >= 3:
				column_identifier = content[2]
				if ":" in column_identifier:
					column_family = column_identifier.split(":")[0]
					column_name = column_identifier.split(":")[1]
				else:
					column_family = column_identifier
			if len(content) == 4 and not multiple_keys:
				timestamp = content[3]
			if not multiple_keys:
				total_deleted = hbase.Delete(table_name, row_key,  column_family, column_name, timestamp)
			else:
				splitted_keys = keys_found.split(',')
				total_deleted = 0
				for key in splitted_keys:
					if ":" in key:
						column_family = key.split(":")[0]
						column_name = key.split(":")[1]
					else:
						column_family = key
						column_name = None
					total_deleted += hbase.Delete(table_name, row_key,  column_family, column_name, timestamp)
			print(">> Se han eliminado " + str(total_deleted) + " registros")

		elif comando[0] == "deleteall":
			# example deleteall 'test',1
			comando[1].replace("'","")
			content = "".join(comando[1:])
			content = content.split(",")
			table_name = content[0]
			table_name = table_name.replace("'","")
			print(table_name)
			row_key = None
			if len(content) < 2:
				print(">> Error con el comando")
				continue
			row_key = content[1]
			total_deleted = hbase.Delete(table_name, row_key)
			print(">> Se han eliminado " + str(total_deleted) + " registros")

		elif comando[0] == "count":
			hbase.Create_Test_Table()
			comando[1].replace("'","")
			arguments = comando[1].split(",")
			if len(arguments) < 1:
				print(">> Error con el comando")
				continue
			table_name = arguments[0][1:-1]

			if table_name not in hbase.tables.keys():
				print(">> La tabla '" + table_name + "' no existe")
				continue
			else:
				if not hbase.Count(table_name):
					print(">> La tabla '" + table_name + "' no tiene registros")
				else:
					print(">> La tabla '" + table_name + "' tiene " + str(hbase.Count(table_name)) + " registros")

		elif comando[0] == "truncate":
			comando[1].replace("'","")
			content = "".join(comando[1:])
			content = content.split(",")
			table_name = content[0]
			table_name = table_name.replace("'","")
			
			if hbase.Truncate(table_name):
				print(">> Se ha truncado la tabla '" + table_name + "'")
			else:
				print(">> Ha ocurrido un error")	
		# <======================= EXTRAS (NO PTS) =======================> 
		elif comando[0] == "clear" or comando[0] == "cls":
			clear_screen()

		elif comando[0] == "help":  # Si el comando es "help"
			print(
				"\n\n============ COMANDOS PERMITIDOS ============\n\n"+
				"\n============== FUNCIONES DDL ==============\n"+
				"\n\tcreate 'nombre_tabla','familia_columna1'"+
				"\n\tlist"+
				"\n\tdisable 'nombre_tabla' "+
				"\n\tenable 'nombre_tabla' "+
				"\n\tis_enabled 'nombre_tabla' "+
				"\n\talter 'nombre_tabla', {NAME => 'nuevo_nombre_tabla'}, DEBE HABER ESPACIO ENTRE LA , Y {" +
				"\n\talter 'nombre_tabla', {ADD => 'nueva_columna'}, DEBE HABER ESPACIO ENTRE LA , Y {" +
				"\n\talter 'nombre_tabla', {DELETE => 'columna'}, DEBE HABER ESPACIO ENTRE LA , Y {" +
				"\n\tdrop 'nombre_tabla'"+
				"\n\tdrop_all"+
				"\n\tdescribe 'nombre_tabla'"+
				"\n\n ============= FUNCIONES DML =============\n"+
				"\n\tput 'nombre_tabla',row,'family:column', 'value' ---> EJEMPLO put 'test',2,'general:name','GRANDE'"+
				"\n\tget 'nombre_tabla',1,{COLUMNA=>'columna:valor'} ---> ejemplo {COLUMN => 'movie_details:title'}"+ # ejemplo 
				"\n\tscan 'nombre_tabla', scan 'nombre_tabla',{LIMIT=>2}, scan 'nombre_tabla',{STARTROW=>2,ENDROW=>4}"+
				"\n\tdelete 'nombre_tabla',1"+
				"\n\tdeleteall 'nombre_tabla',1"+
				"\n\tcount 'nombre_tabla'"+
				"\n\ttruncate 'nombre_tabla'"
			)
		else:
			print(f"Comando <{comando[0]}> no reconocido por el simulador. <help> para mas informacion")


if __name__ == "__main__":
	hbase = HBase()
	hbase.cargar_data()
	hbase.Create_Test_Table()
	hbase.Create_Test_Table2()
	main(hbase)