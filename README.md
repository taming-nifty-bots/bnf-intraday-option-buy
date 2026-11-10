# bnf-intraday-option-buy

Both jobs require `tamingnifty==2.4.3` and the annual `NSE:YYYY` document
in `Bots.exchange_calendars`. Startup checks the IST trading date before broker
login, market data, state writes or polling, in paper and live modes. Holidays
and weekends exit successfully; missing/invalid calendars or DB failures abort
nonzero (fail-closed). Seed and verify the current year's calendar before starting.
Offline startup checks: `python test_startup_calendar.py`.

## Durable execution stop

The executor checks `Bots.execution_guards` at startup before polling/login.
Whole CE/PE entries, half-books and full exits reserve scope
`Bots.<instrument_name_lowercase>_<user_name>` until order and strategy writes
finish with acknowledgement. Exceptions stop nonzero and leave pending/failed
guards across restarts; order/exit retry decorators have been removed.
Reconcile broker orders/fills/positions and Mongo first, then manually clear only
the affected scope. Never clear a latch just to restart. An unresolved latch also
prevents automatic exits: manage any open position manually.
Keep one executor per account; this latch is not exactly-once order deduplication
or automatic broker-position reconciliation.
Current order functions still simulate fills; their real broker calls remain
commented out. No live execution was enabled or verified.
Offline checks: `python test_execution_safety.py`.