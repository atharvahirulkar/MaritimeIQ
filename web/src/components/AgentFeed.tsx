import { useEffect, useState } from "react";

type FeedEvent = { agent: string; message: string; level: string; ts: string; data: Record<string, unknown> };

const COLORS: Record<string, string> = {
  orchestrator: "#c9a227",
  ingestion: "#2a8f8a",
  context: "#3d9a6a",
  judge: "#8aa0b2",
  quality: "#c45c5c",
  entity_resolution: "#c9a227",
  transform: "#2a8f8a",
  discovery: "#3d9a6a",
  gold_builder: "#c9a227",
  insight: "#c9a227",
  guard: "#c45c5c",
  review: "#3d9a6a",
  lakehouse: "#8aa0b2",
  eval: "#3d9a6a",
};

export function useFeed(limit = 200) {
  const [events, setEvents] = useState<FeedEvent[]>([]);
  useEffect(() => {
    const es = new EventSource("/api/feed");
    es.onmessage = (m) => {
      const ev = JSON.parse(m.data) as FeedEvent;
      setEvents((prev) => [...prev.slice(-(limit - 1)), ev]);
    };
    return () => es.close();
  }, [limit]);
  return events;
}

export function AgentFeed({ events }: { events: FeedEvent[] }) {
  return (
    <div className="flex h-full flex-col">
      <h3 className="mb-2 text-sm font-semibold text-muted">Agent Feed</h3>
      <div className="flex-1 space-y-1.5 overflow-auto pr-1">
        {events.length === 0 && <p className="text-xs text-muted">No agent events yet.</p>}
        {[...events].reverse().map((ev, i) => (
          <div
            key={`${ev.ts}-${i}`}
            className="rounded border-l-2 bg-panel/70 px-2 py-1.5 text-xs"
            style={{ borderLeftColor: ev.level === "warn" || ev.level === "error" ? "#c45c5c" : "#2a8f8a" }}
          >
            <span className="font-mono text-muted">{ev.ts.slice(11, 19)}</span>{" "}
            <span className="font-semibold" style={{ color: COLORS[ev.agent] ?? "#8aa0b2" }}>
              {ev.agent.replace("_", " ")}
            </span>
            <div className="mt-0.5 break-words">{ev.message}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
