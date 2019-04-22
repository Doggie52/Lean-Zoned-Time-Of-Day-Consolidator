using System;

using NodaTime;

using QuantConnect.Data.Market;

namespace QuantConnect.Data.Consolidators
{

	/// <summary>
	/// Consolidates data at a single time of day only, in a desired timezone.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	/// <typeparam name="TConsolidated"></typeparam>
	public abstract class ZonedTimeOfDayConsolidatorBase<T, TConsolidated> : DataConsolidator<T>
		where T : IBaseData
		where TConsolidated : BaseData
	{
		/// <summary>
		/// Get the time of day to emit/close a consolidated bar.
		/// </summary>
		/// <remarks>TZ: <see cref="CloseTimeZone"/>.</remarks>
		protected readonly LocalTime DailyCloseTime;

		/// <summary>
		/// Get the timezone in which the time specified for close of consolidated bar is specified.
		/// </summary>
		protected readonly DateTimeZone CloseTimeZone;

		/// <summary>
		/// Get the timezone in which the security's exchange is specified.
		/// </summary>
		protected readonly DateTimeZone ExchangeTimeZone;

		/// <summary>
		/// Get or set the working bar used for aggregating the data.
		/// </summary>
		private TConsolidated _workingBar;

		/// <summary>
		/// Get or set the last time we emitted a consolidated bar.
		/// </summary>
		/// <remarks>TZ: <see cref="ExchangeTimeZone"/>.</remarks>
		private DateTime _lastEmit;

		/// <summary>
		/// Creates a consolidator to produce a new <typeparamref name="TConsolidated"/> instance
		/// representing a day starting at the specified time.
		/// </summary>
		/// <param name="dailyCloseTime">The time of day (in desired timezone) to emit/close a consolidated bar.</param>
		/// <param name="closeTimeZone">The desired timezone string in which to specify the close time.</param>
		/// <param name="exchangeTimeZone">The exchange timezone string of the security.</param>
		protected ZonedTimeOfDayConsolidatorBase( TimeSpan dailyCloseTime, string closeTimeZone, string exchangeTimeZone )
		{

			// Create the time of day as a LocalTime (we can't assign it a timezone yet because DST would make this ambiguous)
			DailyCloseTime = new LocalTime( dailyCloseTime.Hours, dailyCloseTime.Minutes, dailyCloseTime.Seconds );

			// Save the timezones down
			CloseTimeZone = DateTimeZoneProviders.Tzdb[closeTimeZone];
			ExchangeTimeZone = DateTimeZoneProviders.Tzdb[exchangeTimeZone];

			_lastEmit = DateTime.MinValue;
		}

		/// <summary>
		/// Gets the type produced by this consolidator.
		/// </summary>
		public override Type OutputType
		{
			get { return typeof( TConsolidated ); }
		}

		/// <summary>
		/// Gets a clone of the data being currently consolidated.
		/// </summary>
		public override IBaseData WorkingData
		{
			get { return _workingBar != null ? _workingBar.Clone() : null; }
		}

		/// <summary>
		/// Event handler that fires when a new piece of data is produced. We define this as a 'new'
		/// event so we can expose it as a <typeparamref name="TConsolidated"/> instead of a <see cref="BaseData"/> instance.
		/// </summary>
		public new event EventHandler<TConsolidated> DataConsolidated;

		/// <summary>
		/// Updates consolidator only at a fixed time of day in a specific timezone.
		/// </summary>
		/// <param name="data">The new data for the consolidator.</param>
		public override void Update( T data )
		{

			// If we have new data, aggregate it onto the working bar
			if ( data.Time >= _lastEmit ) {
				AggregateBar( ref _workingBar, data );
			}

			// Data and working bar are in exchange TZ, let's zone them
			var zonedDataEndTimeDT = ExchangeTimeZone.AtLeniently( CreateLocalDateTime( data.EndTime ) );
			var zonedWorkingBarTimeDT = ExchangeTimeZone.AtLeniently( CreateLocalDateTime( _workingBar.Time ) );

			// The last time we should have emitted at, with respect to the end of the incoming data
			var shouldHaveEmittedAt = RoundDownToLastEmitTime( zonedDataEndTimeDT );

			/// We consolidate if the following condition(s) are met:
			///		1. Incoming bar's end time (<see cref="ZonedDateTime"/>) is same as the
			///		time we should have emitted at.
			///		2. The time we should have emitted at is after the current working bar's
			///		start time.
			if ( InexactCompareTo( zonedDataEndTimeDT, shouldHaveEmittedAt ) == 0 &&
				InexactCompareTo( shouldHaveEmittedAt, zonedWorkingBarTimeDT ) > 0 ) {

				// Set the EndTime accordingly
				var workingTradeBar = _workingBar as QuoteBar;
				workingTradeBar.EndTime = shouldHaveEmittedAt.WithZone( ExchangeTimeZone ).ToDateTimeUnspecified();

				// Fire consolidation event
				OnDataConsolidated( _workingBar );

				// Last emission was when this bar ended
				_lastEmit = _workingBar.EndTime;

				// Reset the working bar
				_workingBar = null;
			}

		}

		/// <summary>
		/// Scans this consolidator to see if it should emit a bar due to time passing.
		/// </summary>
		/// <param name="currentLocalTime">The current time (TZ: <see cref="ExchangeTimeZone"/>).</param>
		public override void Scan( DateTime currentLocalTime )
		{

			// If we don't have a working bar, there is nothing to emit
			if ( _workingBar != null ) {

				// Current time is provided in exchange TZ, let's zone it to that
				var zonedCurrentLocalTimeDT = ExchangeTimeZone.AtLeniently( CreateLocalDateTime( currentLocalTime ) );

				// Get the latest time the bar should have been emitted
				var shouldHaveEmittedAt = RoundDownToLastEmitTime( zonedCurrentLocalTimeDT );

				// Working bar time is in [exchange] TZ, let's zone it
				var zonedWorkingBarTimeDT = ExchangeTimeZone.AtLeniently( CreateLocalDateTime( _workingBar.Time ) );

				/// We consolidate if the following condition(s) are met:
				///		1. Current local time (exchange TZ) is same as the last time we should
				///		have emitted at.
				///		2. The time we should have emitted as is after the current working bar's
				///		start time.
				if ( InexactCompareTo( zonedCurrentLocalTimeDT, shouldHaveEmittedAt ) == 0 &&
					InexactCompareTo( shouldHaveEmittedAt, zonedWorkingBarTimeDT ) > 0 ) {

					// Update the EndTime of the working tradebar
					var workingTradeBar = _workingBar as QuoteBar;
					workingTradeBar.EndTime = shouldHaveEmittedAt.WithZone( ExchangeTimeZone ).ToDateTimeUnspecified();

					// Fire consolidation event
					OnDataConsolidated( _workingBar );

					// Last emission needs to go back to exchange TZ
					_lastEmit = shouldHaveEmittedAt.WithZone( ExchangeTimeZone ).ToDateTimeUnspecified();

					// Reset working bar
					_workingBar = null;
				}
			}
		}

		/// <summary>
		/// Aggregates the new 'data' into the 'workingBar'. The 'workingBar' will be
		/// null following the event firing.
		/// </summary>
		/// <param name="workingBar">The bar we're building, null if the event was just fired and we're starting a new consolidated bar.</param>
		/// <param name="data">The new data.</param>
		protected abstract void AggregateBar( ref TConsolidated workingBar, T data );

		/// <summary>
		/// Round down a given zoned datetime to the nearest time of emission.
		/// </summary>
		/// <param name="zonedBarTime">The zoned bar datetime to be rounded down.</param>
		/// <returns>The rounded down and zoned bar time (TZ: <see cref="CloseTimeZone"/>).</returns>
		protected ZonedDateTime RoundDownToLastEmitTime( ZonedDateTime zonedBarTime )
		{

			// Convert time given to the same timezone as the close specified if it isn't already
			var zonedConvertedBarTimeDT = zonedBarTime.WithZone( CloseTimeZone );

			// Get timezoned close (now that we have a date)
			var zonedDailyCloseDT = CloseTimeZone.AtLeniently( zonedConvertedBarTimeDT.Date + DailyCloseTime );

			// Return the last close, zoned into the close timezone
			return zonedConvertedBarTimeDT.TimeOfDay >= zonedDailyCloseDT.TimeOfDay ? zonedDailyCloseDT : zonedDailyCloseDT - Duration.FromStandardDays( 1 );
		}

		/// <summary>
		/// Event invocator for the <see cref="DataConsolidated"/> event
		/// </summary>
		/// <param name="e">The consolidated data</param>
		protected virtual void OnDataConsolidated( TConsolidated e )
		{
			base.OnDataConsolidated( e );
			var handler = DataConsolidated;
			if ( handler != null )
				handler( this, e );
		}

		/// <summary>
		/// Create a <see cref="LocalDateTime"/> from a given <see cref="DateTime"/>.
		/// </summary>
		/// <param name="inp">Input <see cref="DateTime"/>.</param>
		/// <returns>A <see cref="LocalDateTime"/> created from the given input.</returns>
		protected LocalDateTime CreateLocalDateTime( DateTime inp )
		{
			return new LocalDateTime( inp.Year, inp.Month, inp.Day, inp.Hour, inp.Minute, inp.Second, inp.Millisecond );
		}

		/// <summary>
		/// Checks whether two zoned datetimes are roughly the same (i.e. within some number of
		/// seconds of each other) or greater/smaller.
		/// </summary>
		/// <param name="zdt1">First zoned datetime to compare.</param>
		/// <param name="zdt2">Second zoned datetime to compare.</param>
		/// <returns>1: zdt1 > zdt2, -1: zdt1 < zdt2, 0: roughly the same</returns>
		protected int InexactCompareTo( ZonedDateTime zdt1, ZonedDateTime zdt2 )
		{
			int seconds = 30;

			// Convert to ticks (10,000 ticks in a millisecond)
			int ticks = seconds * (int)1e3 * (int)1e4;

			// Roughly the same
			if ( Math.Abs( zdt1.ToInstant().Ticks - zdt2.ToInstant().Ticks ) <= ticks )
				return 0;
			else
				return zdt1.ToInstant().Ticks > zdt2.ToInstant().Ticks ? 1 : -1;
		}
	}
}