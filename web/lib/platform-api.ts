/**
 * Platform API client — talks to the platform container (port 8100)
 * through Next.js rewrites (/api/platform/* → http://platform:8100/*).
 *
 * All paths are relative to /api/platform/ so they flow through the
 * rewrite proxy and keep the same origin (no CORS needed in production).
 */

import { apiFetch } from "@/lib/api";

// ── Types ────────────────────────────────────────────────────────

export interface MasterySummary {
  total_kp: number;
  mastered: number;
  learning: number;
  weak: number;
  weak_kps: string[];
  total_questions: number;
  accuracy: number;
}

export interface WeakPoint {
  kp_id: string;
  level: number;
  total: number;
  correct: number;
}

export interface WrongAnswer {
  kp_id: string;
  question: string;
  user_answer: string;
  correct_answer: string;
  ts: number;
}

export interface DayStat {
  date: string;
  total: number;
  correct: number;
  wrong: number;
}

export interface PeriodStats {
  days: number;
  total: number;
  correct: number;
  wrong: number;
  accuracy: number;
  per_day: DayStat[];
  weak_points: string[];
}

export interface AnswerRecord {
  kp_id: string;
  question: string;
  user_answer: string;
  correct_answer: string;
  is_correct: boolean;
  ts: number;
}

export interface PracticeQuestion {
  question: string;
  options?: string[];
  answer: string;
  explanation?: string;
}

export interface PracticeResult {
  ok: boolean;
  kp_id: string;
  questions: PracticeQuestion[];
  total: number;
  trace_id: string;
}

export interface ExamSection {
  type: string;
  count: number;
  questions: string[];
}

export interface ExamResult {
  ok: boolean;
  exam_text: string;
  title: string;
  kp_covered: string[];
  total: number;
  sections: ExamSection[];
  trace_id: string;
}

// ── Helpers ──────────────────────────────────────────────────────

function platformUrl(path: string): string {
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return `/api/platform${normalized}`;
}

async function asJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      if (body?.detail) detail = String(body.detail);
    } catch {
      /* ignore */
    }
    throw new Error(detail);
  }
  return response.json() as Promise<T>;
}

// ── Learners ─────────────────────────────────────────────────────

export async function listLearners(): Promise<string[]> {
  const resp = await apiFetch(platformUrl("/mastery/"));
  return asJson<string[]>(resp);
}

// ── Mastery ──────────────────────────────────────────────────────

export async function fetchMasterySummary(
  learnerId: string,
): Promise<MasterySummary> {
  const resp = await apiFetch(platformUrl(`/mastery/${learnerId}`));
  return asJson<MasterySummary>(resp);
}

export async function fetchWeakPoints(
  learnerId: string,
): Promise<WeakPoint[]> {
  const resp = await apiFetch(platformUrl(`/mastery/${learnerId}/weak`));
  const data = await asJson<{ weak_points: WeakPoint[] }>(resp);
  return data.weak_points;
}

export async function fetchWrongAnswers(
  learnerId: string,
  kpId?: string,
  limit = 10,
): Promise<WrongAnswer[]> {
  const params = new URLSearchParams();
  if (kpId) params.set("kp_id", kpId);
  params.set("limit", String(limit));
  const resp = await apiFetch(
    platformUrl(`/mastery/${learnerId}/wrong?${params}`),
  );
  return asJson<WrongAnswer[]>(resp);
}

export async function fetchWeeklyStats(
  learnerId: string,
): Promise<PeriodStats> {
  const resp = await apiFetch(
    platformUrl(`/mastery/${learnerId}/stats/weekly`),
  );
  return asJson<PeriodStats>(resp);
}

export async function fetchMonthlyStats(
  learnerId: string,
): Promise<PeriodStats> {
  const resp = await apiFetch(
    platformUrl(`/mastery/${learnerId}/stats/monthly`),
  );
  return asJson<PeriodStats>(resp);
}

export async function fetchAnswerHistory(
  learnerId: string,
  limit = 20,
  kpId?: string,
): Promise<AnswerRecord[]> {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  if (kpId) params.set("kp_id", kpId);
  const resp = await apiFetch(
    platformUrl(`/mastery/${learnerId}/history?${params}`),
  );
  const data = await asJson<{ history: AnswerRecord[] }>(resp);
  return data.history;
}

// ── Practice / Exam ──────────────────────────────────────────────

export async function generatePractice(
  learnerId: string,
  kpId: string,
  count = 3,
): Promise<PracticeResult> {
  const resp = await apiFetch(platformUrl("/practice/generate"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ learner_id: learnerId, kp_id: kpId, count }),
  });
  return asJson<PracticeResult>(resp);
}

export async function generateExam(
  learnerId: string,
  kpId = "",
  count = 10,
): Promise<ExamResult> {
  const resp = await apiFetch(platformUrl("/practice/exam"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ learner_id: learnerId, kp_id: kpId, count }),
  });
  return asJson<ExamResult>(resp);
}

// ── Report ───────────────────────────────────────────────────────

export async function generateReport(
  learnerId: string,
  type: "daily" | "weekly" | "monthly",
): Promise<string> {
  const resp = await apiFetch(platformUrl("/report/generate"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ learner_id: learnerId, type }),
  });
  if (!resp.ok) {
    throw new Error(`${resp.status} ${resp.statusText}`);
  }
  return resp.text();
}
