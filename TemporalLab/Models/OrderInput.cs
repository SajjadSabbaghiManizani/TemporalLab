namespace TemporalLab.Models;

public sealed record OrderInput(
    string OrderId,
    int ItemCount,
    bool RequiresApproval);