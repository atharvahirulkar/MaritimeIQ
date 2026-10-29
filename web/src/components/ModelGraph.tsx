import { Background, Controls, type Edge, MarkerType, type Node, ReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useMemo } from "react";
import type { Graph } from "../api";

const COLORS: Record<string, string> = {
  master: "#c9a227",
  dimension: "#c9a227",
  fact: "#2a8f8a",
  view: "#3d9a6a",
  aggregate: "#8aa0b2",
  table: "#2a8f8a",
};
// Column per kind: entities/dims in the middle, tables that reference them around it.
const COLUMN: Record<string, number> = { table: 0, master: 1, dimension: 0, fact: 1, view: 2, aggregate: 2 };

export function ModelGraph({ graph, height = 520 }: { graph: Graph; height?: number }) {
  const { nodes, edges } = useMemo(() => {
    const perCol: Record<number, number> = {};
    const nodes: Node[] = graph.nodes.map((n) => {
      const col = COLUMN[n.kind] ?? 0;
      const row = (perCol[col] = (perCol[col] ?? -1) + 1);
      return {
        id: n.id,
        position: { x: col * 320, y: row * 64 },
        data: { label: n.id },
        style: {
          background: COLORS[n.kind] ?? "#8aa0b2",
          color: "#071018",
          border: "none",
          borderRadius: 6,
          fontSize: 12,
          fontWeight: 600,
          width: 220,
        },
      };
    });
    const edges: Edge[] = graph.edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.label,
      animated: e.type !== undefined && e.type !== "many_to_one",
      markerEnd: { type: MarkerType.ArrowClosed, color: "#8aa0b2" },
      style: { stroke: "#8aa0b2" },
      labelStyle: { fill: "#e6edf2", fontSize: 10 },
      labelBgStyle: { fill: "#152636" },
    }));
    return { nodes, edges };
  }, [graph]);

  if (!graph.nodes.length) return <p className="text-sm text-muted">No graph yet.</p>;
  return (
    <div className="panel" style={{ height }}>
      <ReactFlow nodes={nodes} edges={edges} fitView minZoom={0.2} nodesConnectable={false}>
        <Background color="#1d3346" gap={24} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
