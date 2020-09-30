import pandas as pd
import time, os
from telegram import Location, ReplyKeyboardMarkup, KeyboardButton

# Me basé en esta librería para armar el bot: 
# https://github.com/python-telegram-bot/python-telegram-bot
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, StringCommandHandler

# Y utilicé esta guía para armar el cálculo de las distancias, porque me pareció la más sencilla:
# https://forum.crosscompute.com/t/how-do-i-get-the-nearest-locations/30
from geopy.distance import lonlat, distance

def start(update, context):
	"""Mensaje de bienvenda.
	Sirve también como disparador para el verdadero paso inicial, que es compartir_ubicación().
    """

	update.message.reply_text(
		'Hola {}.'.format(update.message.from_user.first_name)
	)

	compartir_ubicacion(update, context)

def compartir_ubicacion(update, context):
	"""Genera una botonera para que el usuario pueda compartir su ubicación.
	Al compartirla, se dispara guardar_ubicación().
    """

	boton_compartir_ubicacion = KeyboardButton(text="Compartir ubicación", request_location=True)
	reply_markup = ReplyKeyboardMarkup([[boton_compartir_ubicacion]])

	context.bot.send_message(chat_id=update.message.chat.id, text="¿Podrías compartirme tu ubicación, para poder buscar los cajeros cercanos?", reply_markup=reply_markup)

def menu(update, context, primera_vez=True):
	"""Genera una botonera con las distintas redes de cajeros automáticos (Link o Banelco).
	Al seleccionar cualquier opción, se dispara buscar_cajeros().
	Tiene en cuenta si es o no la primera consulta que realiza, por cuestiones de semántica.
    """

	boton_banelco = KeyboardButton(text="Banelco")
	boton_link = KeyboardButton(text="Link")
	reply_markup = ReplyKeyboardMarkup([[boton_banelco], [boton_link]])

	if (primera_vez):
		mensaje = "¿Qué red de cajeros querés consultar?"
	else:
		mensaje = "¿Querés consultar otra red?"

	context.bot.send_message(chat_id=update.message.chat.id, text=mensaje, reply_markup=reply_markup)

def guardar_ubicacion(update, context):
	"""Guarda de manera temporal la ubicación del usuario.
	Al terminar, dispara menu() para que pueda empezar a realizar las consultas.
	"""
	context.user_data["longitud"] = update.message.location.longitude
	context.user_data["latitud"] = update.message.location.latitude
	menu(update, context)

def sortear_distancia(df, context):
	"""Recibe un DF y le agrega/modifica una columna que almacena la distancia (en metros) entre cada cajero y el usuario.
	Devuelve el DF ordenado por los cajeros más cercanos al usuario.
	"""

	df["distancia"] = df.apply(lambda x: distance(lonlat(*(x['long'], x['lat'])), lonlat(*(context.user_data["longitud"], context.user_data["latitud"]))).meters, axis=1)
	df = df.sort_values('distancia')
	return df

def persistir_consultas(df,red):
	"""Recibe un DF ordenado según proximidad al usuario, y:
	Aumenta el contador de cada sucursal que salió como primera, segunda o tercera opción.
	Persiste la cantidad de posibles extracciones que pudieron haberse realizado en cada sucursal.
	"""

	df.iloc[0, df.columns.get_loc("primer_opcion")] += 1
	df.iloc[1, df.columns.get_loc("segunda_opcion")] += 1
	df.iloc[2, df.columns.get_loc("tercer_opcion")] += 1

	for x in range(0,3):
		caso_uno = df.iloc[x, df.columns.get_loc("primer_opcion")] * 0.7
		caso_dos = df.iloc[x, df.columns.get_loc("segunda_opcion")] * 0.2
		caso_tres = df.iloc[x, df.columns.get_loc("tercer_opcion")] * 0.1

		df.iloc[x, df.columns.get_loc("posibles_extracciones")] = caso_uno + caso_dos + caso_tres

	df.to_pickle(red + '.pkl')
	return df

def mostrar_cajeros(df, update, context):
	"""Recibe un DF ordenado y muestra las tres primeras opciones disponibles para el usuario.
	Genera un mensaje informativo con el nombre del banco, dirección, terminales disponibles, distancia (redondeada en metros) y cantidad de extracciones disponibles.
	Además devuelve la ubicación de cada cajero como objeto, para que se pueda abrir con el servicio de mapas que utilice el dispositivo.
	"""

	for x in range(0,3):


		if (df.iloc[x].terminales == 1):
			txt_terminales = "terminal"
		else:
			txt_terminales = "terminales"


		if (df.iloc[x].posibles_extracciones == 999):
			txt_extracciones = "extracción"
		else:
			txt_extracciones = "extracciones"


		extracciones = (1000 * df.iloc[x].terminales) - round(df.iloc[x].posibles_extracciones)

		update.message.reply_text(
			"""Podes ir a {} {}, que tiene {} {} del {}.\nSon aproximadamente {} metros, y según mis datos le deberían de quedar {} {}""".format(
				df.iloc[x].calle,
				df.iloc[x].altura,
				df.iloc[x].terminales,
				txt_terminales,
				df.iloc[x].banco,
				int(round(df.iloc[x].distancia,-2)),
				int(extracciones),
				txt_extracciones
			)
		)

		context.bot.send_location(chat_id=update.message.chat.id, latitude=df.iloc[x].lat, longitude=df.iloc[x].long)

	menu(update, context, False)

def crear_df(red):
	"""Crea un archivo que permita persistir el DF.
	Al hacerlo se añaden contadores para almacenar la cantidad de veces que una sucursal salió como primera, segunda o tercera opción para los usuarios.
	También se inicializa un contador de las posibles extracciones realizadas en cada sucursal.
	Además, se filtran los cajeros que no pertenezcan a CABA.
	"""

	#data_cajeros = pd.read_csv('http://cdn.buenosaires.gob.ar/datosabiertos/datasets/cajeros-automaticos/cajeros-automaticos.csv')
	data_cajeros = pd.read_csv('cajeros-automaticos.csv')
	df = data_cajeros[['long','lat','banco','red','localidad','terminales','calle','altura']]
	df = df.loc[df['localidad'] == "CABA"]
	
	df["primer_opcion"] = 0
	df["segunda_opcion"] = 0
	df["tercer_opcion"] = 0
	df["posibles_extracciones"] = 0

	df.to_pickle(red + '.pkl')

	return df

def read_df(red):
	"""Lee el DF.
	Si existe un df persistido para la red consultada, se lo devuelve.
	Si no existe, o se lo considera desactualizado, entonces se dispara crear_df()
	"""

	if (
		not (os.path.exists(red + '.pkl'))
		or (
			time.localtime(os.path.getmtime(red + '.pkl')).tm_hour < 8
			and time.localtime().tm_hour > 8
			)
		):

		df = crear_df(red)

	df = pd.read_pickle(red + '.pkl')
	return df

def buscar_cajeros(update, context):
	"""Dispara las funciones necesarias para devolverle al usuario sus mejores opciones.
	"""

	# Si por algún motivo antes de empezar no está almacenada su ubicación, se lo devuelve a compartir_ubicación()
	if not (("latitud" in context.user_data) or ("longitud" in context.user_data)):
		return compartir_ubicacion(update, context)


	red = update.message.text.upper()

	df = read_df(red)

	if (red == "BANELCO") or (red == "LINK"):
		df = df.loc[df['red'] == red]
		df = sortear_distancia(df, context)
		df = df[(df['distancia'] < 500) & (df['posibles_extracciones'] < 1000)]

		mostrar_cajeros(df, update, context)
		persistir_consultas(df, red)

	else:
		# Ante cualquier imprevisto, lo devuelvo a compartir_ubicación() para reiniciar el proceso
		return compartir_ubicacion(update, context)


updater = Updater('TOKEN_TELEGRAM', use_context=True)

updater.dispatcher.add_handler(CommandHandler('start', start))
updater.dispatcher.add_handler(MessageHandler(Filters.location, guardar_ubicacion))
updater.dispatcher.add_handler(MessageHandler(Filters.text, buscar_cajeros))

updater.start_polling()
updater.idle()