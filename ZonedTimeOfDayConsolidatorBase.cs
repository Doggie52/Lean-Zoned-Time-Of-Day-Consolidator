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
		/// <remarks>TZ: specified in <see cref="CloseTimeZone"/>.</remarks>
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
		/// Get the timezone in which the security's data is specified.
		/// </summary>
		protected readonly DateTimeZone DataTimeZone;

		/// <summary>
		/// Get or set the working bar used for aggregating the data.
		/// </summary>
		private TConsolidated _workingBar;

		/// <summary>
		/// Get or set the last time we emitted a consolidated bar.
		/// </summary>
		/// <remarks>TZ: security's LocalTime (for Oanda FX: America/New_York).</remarks>
		private DateTime _lastEmit;

		/// <summary>
		/// Creates a consolidator to produce a new <typeparamref name="TConsolidated"/> instance
		/// representing a day starting at the specified time.
		/// </summary>
		/// <param name="dailyCloseTime">The time of day (in desired timezone) to emit/close a consolidated bar.</param>
		/// <param name="closeTimeZone">The desired timezone string in which to specify the close time.</param>
		/// <param name="exchangeTimeZone">The exchange timezone string of the security.</param>
		/// <param name="dataTimeZone">The data timezone string of the security.</param>
		protected ZonedTimeOfDayConsolidatorBase( TimeSpan dailyCloseTime, string closeTimeZone, string exchangeTimeZone, string dataTimeZone )
		{

			// Create the time of day as a LocalTime (we can't assign it a timezone yet because DST would make this ambiguous)
			DailyCloseTime = new LocalTime( dailyCloseTime.Hours, dailyCloseTime.Minutes, dailyCloseTime.Seconds );

			// Save the timezones down
			CloseTimeZone = DateTimeZoneProviders.Tzdb[closeTimeZone];
			ExchangeTimeZone = DateTimeZoneProviders.Tzdb[exchangeTimeZone];
			DataTimeZone = DateTimeZoneProviders.Tzdb[dataTimeZone];

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

			if ( _workingBar != null ) {

				// Data and working bar are in [exchange] TZ, let's zone them
				var zonedDataTimeDT = ExchangeTimeZone.AtLeniently( CreateLocalDateTime( data.Time ) );
				var zonedWorkingBarTimeDT = ExchangeTimeZone.AtLeniently( CreateLocalDateTime( _workingBar.Time ) );

				// The next bar's starting time must be newer than that of the last bar we made and time of day must be right
				if ( GetRoundedBarTime( zonedDataTimeDT ).CompareTo( zonedWorkingBarTimeDT ) > 0 &&
					GetRoundedBarTime( zonedDataTimeDT ).CompareTo( zonedDataTimeDT ) == 0 ) {

					// Set the EndTime accordingly
					var workingTradeBar = _workingBar as QuoteBar;
					workingTradeBar.EndTime = GetRoundedBarTime( zonedDataTimeDT ).WithZone( ExchangeTimeZone ).ToDateTimeUnspecified();

					// Fire consolidation event
					OnDataConsolidated( _workingBar );

					// Last emission was when this bar ended
					_lastEmit = _workingBar.EndTime;

					// Reset the working bar
					_workingBar = null;
				}
			}

			// If we have new data, aggregate it onto the working bar
			if ( data.Time >= _lastEmit ) {
				AggregateBar( ref _workingBar, data );
			}

		}

		/// <summary>
		/// Scans this consolidator to see if it should emit a bar due to time passing.
		/// </summary>
		/// <param name="currentLocalTime">The current time in the exchange time zone.</param>
		public override void Scan( DateTime currentLocalTime )
		{

			// If we don't have a working bar, there is nothing to emit
			if ( _workingBar != null ) {

				// Current time is provided in exchange TZ, let's zone it to that
				var zonedCurrentLocalTimeDT = ExchangeTimeZone.AtLeniently( CreateLocalDateTime( currentLocalTime ) );

				// Get the latest time the bar should have been emitted
				var shouldHaveEmittedAt = GetRoundedBarTime( zonedCurrentLocalTimeDT );

				// Working bar time is in [exchange] TZ, let's zone it
				var zonedWorkingBarTimeDT = ExchangeTimeZone.AtLeniently( CreateLocalDateTime( _workingBar.Time ) );

				// If we should have emitted by now and the time of day is right for doing so
				if ( shouldHaveEmittedAt.CompareTo( zonedWorkingBarTimeDT ) > 0 &&
					shouldHaveEmittedAt.CompareTo( zonedCurrentLocalTimeDT ) == 0 ) {

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
		/// In effect get the latest time a bar should have been emitted. Needs the input to be timezoned.
		/// </summary>
		/// <param name="zonedBarTime">The bar time to be rounded down (in a zoned datetime).</param>
		/// <returns>The rounded and zoned bar time (in the same TZ as the daily close time).</returns>
		protected ZonedDateTime GetRoundedBarTime( ZonedDateTime zonedBarTime )
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
	}
}
