using System;

using QuantConnect.Data.Market;

namespace QuantConnect.Data.Consolidators
{

	/// <summary>
	/// A quote bar consolidator that consolidates data at one (and only one) time every day.
	/// </summary>
	public class ZonedTimeOfDayQuoteBarConsolidator : ZonedTimeOfDayConsolidatorBase<QuoteBar, QuoteBar>
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="ZonedTimeOfDayQuoteBarConsolidator"/> class.
		/// </summary>
		/// <param name="dailyCloseTime">The time of day (in desired timezone) to emit/close a consolidated bar.</param>
		/// <param name="closeTimeZone">The desired timezone string in which to specify the close time.</param>
		/// <param name="exchangeTimeZone">The exchange timezone string of the security.</param>
		public ZonedTimeOfDayQuoteBarConsolidator( TimeSpan dailyCloseTime, string closeTimeZone = "UTC", string exchangeTimeZone = "America/New_York" )
			: base( dailyCloseTime, closeTimeZone, exchangeTimeZone )
		{
		}

		/// <summary>
		/// Aggregates the new 'data' into the 'workingBar'. The 'workingBar' will be
		/// null following the event firing.
		/// </summary>
		/// <param name="workingBar">The bar we're building, null if the event was just fired and we're starting a new consolidated bar.</param>
		/// <param name="data">The new data.</param>
		protected override void AggregateBar( ref QuoteBar workingBar, QuoteBar data )
		{
			var bid = data.Bid;
			var ask = data.Ask;

			if ( workingBar == null ) {

				// Data is in exchange TZ, let's zone it
				var zonedDataTimeDT = ExchangeTimeZone.AtLeniently( CreateLocalDateTime( data.Time ) );

				workingBar = new QuoteBar
				{
					Symbol = data.Symbol,
					Time = RoundDownToLastEmitTime( zonedDataTimeDT ).WithZone( ExchangeTimeZone ).ToDateTimeUnspecified(),
					Bid = bid == null ? null : bid.Clone(),
					Ask = ask == null ? null : ask.Clone(),
					Period = TimeSpan.FromDays( 1 )
				};
			}

			// Update the bid and ask
			if ( bid != null ) {
				workingBar.LastBidSize = data.LastBidSize;
				if ( workingBar.Bid == null ) {
					workingBar.Bid = new Bar( bid.Open, bid.High, bid.Low, bid.Close );
				} else {
					workingBar.Bid.Close = bid.Close;
					if ( workingBar.Bid.High < bid.High )
						workingBar.Bid.High = bid.High;
					if ( workingBar.Bid.Low > bid.Low )
						workingBar.Bid.Low = bid.Low;
				}
			}
			if ( ask != null ) {
				workingBar.LastAskSize = data.LastAskSize;
				if ( workingBar.Ask == null ) {
					workingBar.Ask = new Bar( ask.Open, ask.High, ask.Low, ask.Close );
				} else {
					workingBar.Ask.Close = ask.Close;
					if ( workingBar.Ask.High < ask.High )
						workingBar.Ask.High = ask.High;
					if ( workingBar.Ask.Low > ask.Low )
						workingBar.Ask.Low = ask.Low;
				}
			}

			workingBar.Value = data.Value;
			workingBar.Period += data.Period;
		}

	}
}