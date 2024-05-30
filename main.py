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
			nombre_tabla = comando[1][1:-1]
			contenido = " ".join(comando[2:])
			contenido = contenido.split(",")
			if hbase.Put(nombre_tabla, contenido):
				print(f">> Se ha insertado el registro en la tabla <{nombre_tabla}>")
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")

		elif comando[0] == "get":
			nombre_tabla = comando[1][1:-1]
			comando[1].replace("'","")
			res = hbase.Get(nombre_tabla, comando[2], comando[3], comando[4])
			if res:
				print(f">> Se ha obtenido la informacion <{nombre_tabla}>")
				print(res)
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")

		elif comando[0] == "scan":
			nombre_tabla = comando[1][1:-1]
			comando[1].replace("'","")
			args = comando[1].split(",")
			if len(args) < 1:
				print(">> Error con el comando")
				continue
			nombre_tabla = args[0][1:-1]
			largo = len(args)
			inicio, fin, limite = None, None, None
			if len(args) == 3:
				if args[1][0] != '{' and args[2][-1] != '}':
					print(">> Error con el comando")
					continue
				else:
					arg1 = args[1][1:]
					arg2 = args[2][:-1]

					if '=>' not in arg1 or '=>' not in arg2:
						print(">> Error con el comando")
						continue
					else:
						inicio = arg1.split('=>')[1]
						fin = arg2.split('=>')[1]
						inicio = int(inicio)
						fin = int(fin)
						clave_inicio = arg1.split('=>')[0]
						clave_fin = arg2.split('=>')[0]
						if clave_inicio != 'STARTROW' or clave_fin != 'ENDROW':
							print(">> Error con el comando")
							continue

			if len(args) == 2:
				if args[1][0] != '{' and args[1][-1] != '}':
					print(">> Error con el comando")
					continue
				else:
					arg1 = args[1][1:-1]
					if '=>' not in arg1:
						print(">> Error con el comando")
						continue
					else:
						limite = arg1.split('=>')[1]
						limite = int(limite)
						keyword_lim = arg1.split('=>')[0]
						if keyword_lim != 'LIMIT':
							print(">> Error con el comando")
							continue	

			if nombre_tabla not in hbase.tables.keys():
				print(">> La tabla '" + nombre_tabla + "' no existe")
				continue
			else:
				if inicio and fin:
					if inicio > fin:
						print(">> Error con el comando, rangos no validos")
						continue
					elif inicio == fin:
						print(">> Error con el comando, STARTROW y ENDROW no pueden ser iguales")
						continue
					elif not hbase.Scan(table_name=nombre_tabla, row_start=inicio, row_stop=fin):
						print(">> La tabla '" + nombre_tabla + "' no tiene registros en ese rango")
					else:
						for row in hbase.Scan(table_name=nombre_tabla, row_start=inicio, row_stop=fin):
							if row.enabled:
								print(" Key:" + str(row.key) + " value:" + str(row.value) + " timestamp:" + str(row.timestamp))
				else:
					if limite:
						if not hbase.Scan(table_name=nombre_tabla, limit=limite):
							print(">> La tabla '" + nombre_tabla + "' no tiene registros con las especificaciones dadas")
						else:
							for row in hbase.Scan(table_name=nombre_tabla, limit=limite):
								if row.enabled:
									print(" Key:" + str(row.key) + " value:" + str(row.value) + " timestamp:" + str(row.timestamp))
					else:
						if not hbase.Scan(nombre_tabla):
							print(">> La tabla '" + nombre_tabla + "' no tiene registros con las especificaciones dadas")
						else:
							for row in hbase.Scan(nombre_tabla):
								if row.enabled:
									print(" Key:" + str(row.key) + " value:" + str(row.value) + " timestamp:" + str(row.timestamp))
		elif comando[0] == "delete":
			nombre_tabla = comando[1][1:-1]
			comando[1].replace("'","")
			if hbase.Delete_table(nombre_tabla):
				print(f">> Se ha borrado la tabla <{nombre_tabla}>")
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")

		elif comando[0] == "deleteall":
			comando[1].replace("'","")
			nombre_tabla = comando[1][1:-1]
			if hbase.Delete_all(nombre_tabla):
				print(f">> Se han eliminado todos los registros de la tabla <{nombre_tabla}>")
			else:
				print(f">> Ha ocurrido un error, tabla inexistente")

		elif comando[0] == "count":
			comando[1].replace("'","")
			argumentos = comando[1].split(",")
			if len(argumentos) < 1:
				print(f"Hay un error con el comando ingresado")
				continue
			nombre_tabla = argumentos[0][1:-1]
			if nombre_tabla not in hbase.tables.keys():
				print(f">> Tabla <{nombre_tabla}> no registrada. Creela")
				continue
			else:
				if not hbase.Count(nombre_tabla):
					print(f">> Tabla <{nombre_tabla}> sin registros")
				else:
					print(f">> Tabla <{nombre_tabla}> posee {str(hbase.Count(nombre_tabla))} registros.")

		elif comando[0] == "truncate":
			comando[1].replace("'","")
			argumentos = comando[1].split(",")
			if hbase.Truncate(nombre_tabla):
				print(f">> Se ha truncado la tabla <{nombre_tabla}>")
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")

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
				"\n\tput"+
				"\n\tget"+
				"\n\tscan"+
				"\n\tdelete"+
				"\n\tdelete_all"+
				"\n\tcount"+
				"\n\ttruncate"
			)
		else:
			print(f"Comando <{comando[0]}> no reconocido por el simulador. <help> para mas informacion")


if __name__ == "__main__":
	hbase = HBase()
	hbase.cargar_data()
	main(hbase)