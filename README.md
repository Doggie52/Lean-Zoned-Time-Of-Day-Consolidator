# Lean-Zoned-Time-Of-Day-Consolidator

*LEAN Zoned Time Of Day Consolidator* is an unofficial, alternative consolidator for [QuantConnect's Lean Engine](https://github.com/quantconnect/lean) that consolidates bars *once a day* at a *specific time of day* in a *given timezone*.

Precise bar consolidation is important in many aspects of trading, in particular in Forex and Commodity markets that tend to trade throughout the day in different timezones.

## Getting Started

Usage:

 * Set your data resolution to at least `Resolution.Hour`. If your time of day has a minute-component, you will need `Resolution.Minute`. Same if your time of day has a second-component (`Resolution.Second`).
 * Example: consolidate bars at 3AM London time every day:
```c#
MyConsolidator = new ZonedTimeOfDayQuoteBarConsolidator(
	dailyCloseTime: TimeSpan.FromHours( 3 ),
	closeTimeZone: "Europe/London",
	exchangeTimeZone: security.Exchange.TimeZone.ToString()
);
```