import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import sys
import io

# Configurar codificación UTF-8 para la consola
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ===== CONFIGURACIÓN =====
URL_WEBSERVICE = "http://gpsmobile.co:2998/"
USUARIO = "Wsasservi"
CLAVE = "Wsasservi*2026"
FORMULARIO = "INSPECCIÓN INTEGRADA RE-04-GO V.13"

# CONFIGURACIÓN DE FECHAS AUTOMÁTICAS
DIAS_ATRAS = 1  # Número de días hacia atrás para consultar (ajusta según necesidad)
INCLUIR_HOY = True

# CONFIGURACIÓN DE ARCHIVO
CARPETA_SALIDA = "G:\\My Drive\\Dataset Python"
NOMBRE_ARCHIVO_FIJO = "datos_gps_cesem.csv"
GUARDAR_HISTORICO = False

# Columnas que identifican unívocamente un registro.
COLUMNAS_CLAVE = ['Ticket', 'Placa', 'FHInicio']

# Columnas que contienen IDs o cédulas: pandas las convierte a float (ej. 123456.0)
# y hay que limpiarlas a entero-string (ej. "123456").
# Agrega aquí cualquier columna nueva que tenga el mismo problema.
COLUMNAS_ID = ['Ticket', 'CedulaConductor']

# Columnas de coordenadas geográficas (deben quedar como float, no se tocan).
# Se listan aquí solo como referencia para documentar las columnas nuevas.
COLUMNAS_GEO = ['Latitud', 'Longitud']
# =========================


def calcular_fechas():
    ahora = datetime.now()
    if INCLUIR_HOY:
        fecha_final = ahora
    else:
        fecha_final = ahora - timedelta(days=1)
    fecha_inicial = fecha_final - timedelta(days=DIAS_ATRAS)
    return (
        fecha_inicial.strftime("%d/%m/%Y 00:00:00"),
        fecha_final.strftime("%d/%m/%Y 23:59:59"),
    )


def llamar_soap_gethistoricocesem(usuario, clave, fh_inicial, fh_final, formulario=""):
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetHistoricoCesem xmlns="http://190.24.175.109:2998/">
      <sUsuario>{usuario}</sUsuario>
      <sClave>{clave}</sClave>
      <sFHInicial>{fh_inicial}</sFHInicial>
      <sFHFinal>{fh_final}</sFHFinal>
      <sFormulario>{formulario}</sFormulario>
    </GetHistoricoCesem>
  </soap:Body>
</soap:Envelope>"""

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'http://190.24.175.109:2998/GetHistoricoCesem'
    }

    try:
        print("   Enviando petición al servidor...")
        response = requests.post(URL_WEBSERVICE, data=soap_body, headers=headers, timeout=30)
        print(f"   Status code: {response.status_code}")

        if response.status_code != 200:
            print(f"\n   ❌ Error del servidor:\n   {response.text[:2000]}")
            return None

        root = ET.fromstring(response.content)
        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns': 'http://190.24.175.109:2998/'
        }

        resultado = root.find('.//ns:GetHistoricoCesemResult', namespaces)
        if resultado is not None and resultado.text:
            return resultado.text

        for elem in root.iter():
            if 'GetHistoricoCesemResult' in elem.tag and elem.text:
                return elem.text

        print("   ⚠️ No se encontró resultado en la respuesta")
        return None

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None


def limpiar_columnas_id(df):
    """
    FIX: Convierte columnas de ID/cédula de float64 a string limpio.

    El problema: pandas convierte columnas numéricas con algún nulo a float64,
    lo que agrega '.0' al final (ej. 123456789.0 en lugar de 123456789).

    La solución en 3 pasos:
      1. to_numeric(errors='coerce')  → estandariza el tipo, fuerza NaN en inválidos
      2. astype('Int64')              → Int64 nullable admite NaN sin usar float
      3. astype(str).replace('<NA>','') → convierte a texto, NaN queda como vacío

    Latitud y Longitud NO se tocan porque deben conservar sus decimales.
    """
    for col in COLUMNAS_ID:
        if col not in df.columns:
            continue
        df[col] = (
            pd.to_numeric(df[col], errors='coerce')
            .astype('Int64')
            .astype(str)
            .replace('<NA>', '')
        )
        print(f"   🔢 {col} → convertida a texto sin decimales")
    return df


def procesar_json_a_dataframe(json_text):
    if not json_text:
        return None
    try:
        datos = json.loads(json_text)

        if isinstance(datos, list):
            df = pd.DataFrame(datos)
        elif isinstance(datos, dict):
            for key, value in datos.items():
                if isinstance(value, list):
                    df = pd.DataFrame(value)
                    break
            else:
                df = pd.DataFrame([datos])
        else:
            df = pd.DataFrame([datos])

        # FIX: limpiar columnas ID antes de retornar
        df = limpiar_columnas_id(df)

        return df

    except Exception as e:
        print(f"   ❌ Error procesando datos: {e}")
        return None


def combinar_con_historico(df_nuevo, carpeta_salida, archivo_historico):
    """
    CORRECCIONES:
    1. Limpia filas fantasma antes del concat.
    2. drop_duplicates por 'Ticket' (clave de negocio), no por todas las columnas.
    3. Aplica limpiar_columnas_id al histórico al leerlo, por si fue guardado
       antes de que existiera el fix (evita mezclar '123456' con '123456.0').
    """
    ruta_completa = os.path.join(carpeta_salida, archivo_historico)

    # Limpiar filas fantasma del lote nuevo
    cols_clave_presentes = [c for c in COLUMNAS_CLAVE if c in df_nuevo.columns]
    if cols_clave_presentes:
        antes = len(df_nuevo)
        df_nuevo = df_nuevo.dropna(subset=cols_clave_presentes)
        eliminadas = antes - len(df_nuevo)
        if eliminadas > 0:
            print(f"   🧹 Filas fantasma eliminadas del nuevo lote: {eliminadas}")
    else:
        print(f"   ⚠️ Columnas clave no encontradas: {COLUMNAS_CLAVE}")

    print(f"   ✅ Registros válidos en nuevo lote: {len(df_nuevo)}")

    if os.path.exists(ruta_completa):
        try:
            df_historico = pd.read_csv(ruta_completa, encoding='utf-8-sig', index_col=False)

            # Aplicar fix de ID al histórico también (por si fue guardado sin el fix)
            df_historico = limpiar_columnas_id(df_historico)

            cols_clave_hist = [c for c in COLUMNAS_CLAVE if c in df_historico.columns]
            if cols_clave_hist:
                df_historico = df_historico.dropna(subset=cols_clave_hist)

            print(f"   📂 Archivo histórico: {len(df_historico)} registros válidos")

            df_combinado = pd.concat([df_historico, df_nuevo], ignore_index=True)

            # Deduplicar por clave de negocio
            antes = len(df_combinado)
            if 'Ticket' in df_combinado.columns:
                df_combinado = df_combinado.drop_duplicates(subset=['Ticket'], keep='last')
                print(f"   🔄 Duplicados por Ticket eliminados: {antes - len(df_combinado)}")
            else:
                cols_dedup = [c for c in COLUMNAS_CLAVE if c in df_combinado.columns]
                df_combinado = df_combinado.drop_duplicates(
                    subset=cols_dedup if cols_dedup else None, keep='last'
                )

            print(f"   ✅ Total final: {len(df_combinado)} registros")
            return df_combinado

        except Exception as e:
            print(f"   ⚠️ Error leyendo histórico: {e}")
            return df_nuevo
    else:
        print(f"   📝 No existe archivo histórico, creando uno nuevo")
        return df_nuevo


def exportar_datos(df, carpeta_salida, nombre_fijo, guardar_historico=True):
    if df is None or df.empty:
        print("\n   ⚠️ No hay datos para exportar")
        return False

    if not os.path.exists(carpeta_salida):
        os.makedirs(carpeta_salida)
        print(f"   📁 Carpeta creada: {carpeta_salida}")

    archivos_generados = []

    try:
        ruta_fija = os.path.join(carpeta_salida, nombre_fijo)
        df.to_csv(ruta_fija, index=False, encoding='utf-8-sig')
        archivos_generados.append(ruta_fija)
        print(f"\n   ✅ Archivo principal actualizado: {ruta_fija}")

        if guardar_historico:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            archivo_historico = f"historico_gps_{timestamp}.csv"
            ruta_historico = os.path.join(carpeta_salida, archivo_historico)
            df.to_csv(ruta_historico, index=False, encoding='utf-8-sig')
            archivos_generados.append(ruta_historico)
            print(f"   💾 Backup histórico guardado: {ruta_historico}")

        print(f"\n   📊 Total de registros: {len(df)}")
        print(f"   📋 Columnas finales ({len(df.columns)}):")
        for col in df.columns:
            print(f"      • {col}")

        return archivos_generados

    except Exception as e:
        print(f"\n   ❌ Error al exportar: {e}")
        return False


# ============ EJECUCIÓN PRINCIPAL ============
if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("  ACTUALIZACIÓN AUTOMÁTICA - WEBSERVICE GPS CESEM")
    print("=" * 70)

    fecha_inicial, fecha_final = calcular_fechas()

    print(f"\n📅 FECHAS CALCULADAS AUTOMÁTICAMENTE:")
    print(f"   Desde: {fecha_inicial}")
    print(f"   Hasta: {fecha_final}")
    print(f"   (Consultando últimos {DIAS_ATRAS} día(s))")

    print(f"\n📋 CONFIGURACIÓN:")
    print(f"   Usuario: {USUARIO}")
    print(f"   Carpeta de salida: {CARPETA_SALIDA}")
    print(f"   Archivo de salida: {NOMBRE_ARCHIVO_FIJO}")
    print(f"   Guardar histórico: {'Sí' if GUARDAR_HISTORICO else 'No'}")
    print(f"   Clave primaria: {COLUMNAS_CLAVE}")
    print(f"   Columnas ID a limpiar: {COLUMNAS_ID}")

    print(f"\n🔄 PASO 1: Consultando webservice...")
    respuesta = llamar_soap_gethistoricocesem(
        USUARIO, CLAVE, fecha_inicial, fecha_final, FORMULARIO
    )

    if respuesta:
        print("   ✅ Respuesta recibida")

        print("\n🔄 PASO 2: Procesando datos...")
        df_nuevo = procesar_json_a_dataframe(respuesta)

        if df_nuevo is not None and not df_nuevo.empty:
            print(f"   ✅ {len(df_nuevo)} registros nuevos procesados")
            print(f"   📋 Columnas detectadas: {list(df_nuevo.columns)}")

            print("\n🔄 PASO 3: Combinando con datos existentes...")
            df_final = combinar_con_historico(df_nuevo, CARPETA_SALIDA, NOMBRE_ARCHIVO_FIJO)

            print("\n🔄 PASO 4: Exportando datos...")
            archivos = exportar_datos(df_final, CARPETA_SALIDA, NOMBRE_ARCHIVO_FIJO, GUARDAR_HISTORICO)

            if archivos:
                print("\n" + "=" * 70)
                print("  ✅ ACTUALIZACIÓN COMPLETADA EXITOSAMENTE")
                print("=" * 70)
                print("\n📁 ARCHIVOS GENERADOS:")
                for archivo in archivos:
                    print(f"   • {archivo}")

                print("\n📊 PREVIEW DE LOS DATOS:")
                print("-" * 70)
                print(df_final.head(5).to_string())
                print("-" * 70)
        else:
            print("   ⚠️ No se encontraron datos nuevos")
    else:
        print("\n❌ No se pudo obtener respuesta del servidor")

    print("\n" + "=" * 70)
    print(f"  Ejecutado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")