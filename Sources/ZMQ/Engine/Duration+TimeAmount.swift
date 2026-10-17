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

extension Duration {
    /// This duration in tenths of a second, as a PING TTL: rounded down, between 0 and 65535.
    var tenthsOfSecond: UInt16 {
        let (seconds, attoseconds) = components
        guard seconds >= 0 else { return 0 }
        let tenths = seconds.multipliedReportingOverflow(by: 10)
        guard !tenths.overflow else { return .max }
        let total = tenths.partialValue + attoseconds / 100_000_000_000_000_000
        return total > Int64(UInt16.max) ? .max : UInt16(total)
    }
}
