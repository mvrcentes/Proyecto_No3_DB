from HBase import HBase
import time

def main():
	"""
	Función principal que actúa como simulador de comandos para HBase.

	Crea una instancia de HBase y permite la entrada de comandos en un bucle continuo.
	Los comandos admitidos permiten la creación de tablas, listado de tablas, y otras operaciones DDL y DML.
	"""
	hbase = HBase()  # Crea una instancia de HBase
	while True:  # Bucle continuo para recibir comandos
		comando = input(">>")  # Espera por la entrada del comando
		if comando == "exit":  # Si el comando es "exit", termina el simulador
			print("Saliendo", end="", flush=True)
			for i in range(3):  # Realiza la animación de puntos tres veces
				time.sleep(0.5)  # Pausa de 0.5 segundos
				print(".", end="", flush=True)  # Agrega un punto
			print("\nSimulador terminado.")  # Mensaje final
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
			nombre_tabla = comando[1][1:-1]
			contenido = " ".join(comando[2:])
			contenido = contenido.split(",")
			if hbase.Put(nombre_tabla, contenido):
				print(f">> Se ha insertado el registro en la tabla <{nombre_tabla}>")
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")

		elif comando[0] == "get":
			nombre_tabla = comando[1][1:-1]
			res = hbase.Get(nombre_tabla, comando[2], comando[3], comando[4])
			if res:
				print(f">> Se ha obtenido la informacion <{nombre_tabla}>")
				print(res)
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")

		elif comando[0] == "scan":
			nombre_tabla = comando[1][1:-1]
			res = hbase.Scan(nombre_tabla, comando[2], comando[3])
			if res:
				print(f">> Se ha obtenido la informacion <{nombre_tabla}>")
				print(res)
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")

		elif comando[0] == "delete":
			nombre_tabla = comando[1][1:-1]
			if hbase.Delete_table(nombre_tabla):
				print(f">> Se ha borrado la tabla <{nombre_tabla}>")
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")

		elif comando[0] == "deleteall":
			nombre_tabla = comando[1][1:-1]
			if hbase.Delete_all(nombre_tabla):
				print(f">> Se han eliminado todos los registros de la tabla <{nombre_tabla}>")
			else:
				print(f">> Ha ocurrido un error, tabla inexistente")

		elif comando[0] == "count":
			nombre_tabla = comando[1][1:-1]
			res = hbase.Count(nombre_tabla)
			if res:
				print(f">> Se ha obtenido la informacion <{nombre_tabla}>")
				print(res)
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")

		elif comando[0] == "truncate":
			nombre_tabla = comando[1][1:-1]
			if hbase.Truncate(nombre_tabla):
				print(f">> Se ha truncado la tabla <{nombre_tabla}>")
			else:
				print(f">> Ha ocurrido un error, tabla inexistente o columna inexistente")


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
	main()