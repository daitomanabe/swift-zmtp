import NIOCore

extension Duration {
    /// This duration as a NIO `TimeAmount`: never negative, rounded down to nanoseconds,
    /// capped at the largest value `TimeAmount` can hold.
    var timeAmount: TimeAmount {
        let (seconds, attoseconds) = components
        guard seconds >= 0 else { return .zero }
        let (wholeNanoseconds, overflow) = seconds.multipliedReportingOverflow(by: 1_000_000_000)
        if overflow { return .nanoseconds(Int64.max) }
        let (total, addOverflow) = wholeNanoseconds.addingReportingOverflow(attoseconds / 1_000_000_000)
        return .nanoseconds(addOverflow ? Int64.max : total)
    }
}
