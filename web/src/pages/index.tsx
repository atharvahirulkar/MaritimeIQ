import type { ComponentType } from "react";
import type { Status } from "../api";
import { Bronze, Connect, Hitl, Quality, SemanticsPage, Silver, Sources } from "./act1";
import { Ask, Gold, InsightsPage } from "./act2";
import { Trust } from "./trust";

export const PAGES: Record<string, ComponentType<{ status: Status }>> = {
  sources: Sources,
  bronze: Bronze,
  semantics: SemanticsPage,
  quality: Quality,
  hitl: Hitl,
  connect: Connect,
  silver: Silver,
  insights: InsightsPage,
  gold: Gold,
  ask: Ask,
  trust: Trust,
};
