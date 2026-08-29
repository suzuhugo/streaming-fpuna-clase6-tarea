import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.

    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
    
        # raise NotImplementedError("TODO 1: implementar parse_utc")

        if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
            raise ValueError(f"Timestamp UTC inválido: {raw_value!r}")

        try:
            return datetime.fromisoformat(
                raw_value.removesuffix("Z") + "+00:00"
            )
        except ValueError as exc:
            raise ValueError(
                f"Timestamp ISO-8601 inválido: {raw_value!r}"
            ) from exc

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        # raise NotImplementedError("TODO 2: implementar assign_fixed_window")

        epoch_seconds = int(timestamp.timestamp())

        window_start_seconds = (
            epoch_seconds // size_seconds
        ) * size_seconds

        window_end_seconds = window_start_seconds + size_seconds

        window_start = datetime.fromtimestamp(
            window_start_seconds,
            tz=timestamp.tzinfo,
        )

        window_end = datetime.fromtimestamp(
            window_end_seconds,
            tz=timestamp.tzinfo,
        )

        return window_start, window_end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """
        # raise NotImplementedError("TODO 3: implementar summarize_payments")

        totals_by_window = {}
        audit = []
        seen_by_merchant = {}

        for event in events:
            event_id = event["event_id"]
            merchant_id = event["merchant_id"]

            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])

            window_start, window_end = assign_fixed_window(
                event_time,
                window_seconds,
            )

            delay_seconds = (
                arrival_time - event_time
            ).total_seconds()

            merchant_seen = seen_by_merchant.setdefault(
                merchant_id,
                set(),
            )

            duplicate = (
                deduplicate
                and event_id in merchant_seen
            )

            if deduplicate and not duplicate:
                merchant_seen.add(event_id)

            too_late = (
                delay_seconds > allowed_lateness_seconds
            )

            confirmed = event["status"] == "CONFIRMED"

            accepted = (
                confirmed
                and not duplicate
                and not too_late
            )

            revision = (
                accepted
                and arrival_time > window_end
            )

            if duplicate:
                reason = "duplicate"
            elif not confirmed:
                reason = "not_confirmed"
            elif too_late:
                reason = "too_late"
            else:
                reason = "accepted"

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

            if not accepted:
                continue

            key = (
                merchant_id,
                window_start.isoformat(),
                window_end.isoformat(),
            )

            totals_by_window[key] = (
                totals_by_window.get(key, 0)
                + event["amount"]
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
            }
            for (
                merchant_id,
                window_start,
                window_end,
            ), total in sorted(totals_by_window.items())
        ]

        return totals, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """
        # raise NotImplementedError(
        #    "TODO 4: implementar build_windowed_totals_pipeline"
        #)

        created = (
            pipeline
            | "Create payments" >> beam.Create(events)
        )

        timestamped = (
            created
            | "Assign event time" >> beam.Map(
                lambda event: beam.window.TimestampedValue(
                    event,
                    parse_utc(event["event_time"]).timestamp(),
                )
            )
        )

        confirmed = (
            timestamped
            | "Only confirmed" >> beam.Filter(
                lambda event: event["status"] == "CONFIRMED"
            )
        )

        windowed = (
            confirmed
            | "Window per minute" >> beam.WindowInto(
                beam.window.FixedWindows(window_seconds)
            )
        )

        keyed = (
            windowed
            | "Key amount by merchant" >> beam.Map(
                lambda event: (
                    event["merchant_id"],
                    event["amount"],
                )
            )
        )

        summed = (
            keyed
            | "Sum per merchant and window"
            >> beam.CombinePerKey(sum)
        )

        class FormatTotal(beam.DoFn):
            def process(
                self,
                element,
                window=beam.DoFn.WindowParam,
            ):
                merchant_id, total = element
    
                yield {
                    "merchant_id": merchant_id,
                    "window_start": (
                        window.start
                        .to_utc_datetime(has_tz=True)
                        .isoformat()
                    ),
                    "window_end": (
                        window.end
                        .to_utc_datetime(has_tz=True)
                        .isoformat()
                    ),
                    "total": total,
                }

        return (
            summed
            | "Format totals" >> beam.ParDo(
                FormatTotal()
            )
        )

    return


@app.cell
def _(Any, SetStateSpec, StrUtf8Coder, TimeDomain, TimerSpec, beam, on_timer):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            #raise NotImplementedError(
            #    "TODO 5: implementar DeduplicatePayments.process"
            #)

            merchant_id, event = element
            event_id = event["event_id"]

            seen = set(seen_ids.read())

            if event_id in seen:
                return

            seen_ids.add(event_id)

            expiry.set(window.end)

            yield merchant_id, event
        

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            #raise NotImplementedError(
            #    "TODO 5b: implementar DeduplicatePayments.expire"
            #)

            seen_ids.clear()

    return


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """

        from apache_beam.transforms import trigger
        from apache_beam.utils.timestamp import Duration

        class _DurationCompat(Duration):
            @property
            def seconds(self) -> int:
                return self.micros // 1_000_000

        policy = beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            allowed_lateness=allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )

        policy.windowing.windowfn.size = _DurationCompat(
            window_seconds
        )

        policy.windowing.allowed_lateness = _DurationCompat(
            allowed_lateness_seconds
        )

        return policy

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        # raise NotImplementedError("TODO 7: implementar make_idempotency_key")
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, múltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        # raise NotImplementedError("TODO 8: implementar simulate_sink_retries")

        audit = []
        append_sink = []
        upsert_sink = {}

        for result in results:
            idempotency_key = make_idempotency_key(result)

            for attempt in range(1, attempts + 1):
                operation = "UPSERT" if idempotent else "POST"

                row = {
                    **result,
                    "idempotency_key": idempotency_key,
                }

                audit.append(
                    {
                        **row,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )

                if idempotent:
                    upsert_sink[idempotency_key] = row
                else:
                    append_sink.append(row)

        if idempotent:
            materialized = list(upsert_sink.values())
        else:
            materialized = append_sink

        return materialized, audit


    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [ ] un duplicado no modifica el total;
    - [ ] claves distintas no comparten estado;
    - [ ] un evento fuera de orden cae en su ventana de evento;
    - [ ] un evento con atraso aceptado produce una revisión;
    - [ ] un evento demasiado tardío queda auditado;
    - [ ] dos escrituras del mismo resultado dejan una sola entidad;
    - [ ] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    README
    # Tarea 3 – Pipeline tolerante a desorden y reintentos

    Asignatura: Streaming de datos y sus aplicaciones

    ## Objetivo

    Implementar un pipeline de procesamiento de pagos con Apache Beam capaz de
    manejar tiempo de evento, ventanas, eventos tardíos, duplicados, estado,
    timers, triggers y reintentos de escritura.

    El pipeline calcula los totales de pagos `CONFIRMED` por comercio y por
    ventana de un minuto.

    ## Política implementada

    ### Tiempo de evento

    Se utiliza `event_time` como timestamp lógico del evento. Los timestamps
    ISO-8601 terminados en `Z` se convierten a objetos `datetime` UTC
    timezone-aware.

    ### Ventanas

    Se utilizan ventanas fijas de 60 segundos:

    - cada evento pertenece a una única ventana;
    - la asignación se realiza según `event_time`, no según `arrival_time`.

    ### Datos tardíos

    Se admite un `allowed_lateness` de 120 segundos.

    Los eventos tardíos que todavía se encuentran dentro de este margen pueden
    actualizar el resultado de su ventana. Los eventos que superan el límite se
    clasifican como `too_late`.

    ### Triggers y panes

    La política temporal utiliza:

    - `AfterWatermark` para la emisión on-time;
    - `AfterProcessingTime(10)` para una emisión early;
    - `AfterCount(1)` para las revisiones late;
    - modo de acumulación `ACCUMULATING`.

    Por tanto, cada pane representa la versión completa y actualizada del
    resultado de la ventana.

    ### Deduplicación

    La deduplicación se realiza mediante estado administrado por Apache Beam.

    Cada comercio mantiene un conjunto de `event_id` ya observados. El estado
    está aislado por clave, por lo que un mismo `event_id` puede existir en
    comercios diferentes sin producir una falsa detección de duplicado.

    ### Expiración del estado

    Se utiliza un timer en tiempo de evento para limpiar el estado de
    deduplicación y evitar que crezca indefinidamente.

    ### Idempotencia del sink

    La clave de idempotencia utilizada es:

    `merchant_id|window_start`

    Esta clave identifica un único resumen lógico por comercio y ventana.

    En modo UPSERT, varios reintentos de escritura actualizan la misma entidad.
    En cambio, un sink append-only/POST genera una nueva fila en cada intento.

    ## Trade-offs

    La política adoptada prioriza un equilibrio entre latencia, completitud y
    costo:

    - los panes early reducen la latencia, pero son resultados provisionales;
    - 120 segundos de lateness permiten incorporar eventos retrasados y aumentan
      la completitud;
    - mantener lateness y estado durante más tiempo incrementa el uso de memoria
      y el número potencial de revisiones;
    - la deduplicación requiere mantener estado por clave;
    - el UPSERT evita duplicar efectos externos ante reintentos.

    ## Ejecución con uv

    Instalar las dependencias:

    ```bash
    uv sync --frozen

    ### Suite completa

    ![Suite pytest completamente verde](/pytest.png)
    """)
    return


if __name__ == "__main__":
    app.run()
