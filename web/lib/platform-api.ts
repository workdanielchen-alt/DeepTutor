// ── Types ────────────────────────────────────────────────────

export interface MasterySummary {
  total_questions: number;
  accuracy: number;
  mastered: number;
  total_kp: number;
}

export interface WeakPoint {
  kp_id: string;
  level: number;
  total: number;
  correct: number;
}

export interface WrongAnswer {
  question: string;
  user_answer: string;
  correct_answer: string;
  kp_id: string;
}

export interface PeriodStats {
  total: number;
  accuracy: number;
  per_day: { date: string; total: number }[];
}

export interface PracticeQuestion {
  question: string;
  options: string[];
  answer: string;
  explanation?: string;
}

// ── HTTP helpers ─────────────────────────────────────────────

const BASE = "/api/platform";

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`请求失败 (${res.status})`);
  return res.json();
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`请求失败 (${res.status})`);
  return res.json();
}

async function apiPostText(path: string, body: unknown): Promise<string> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`请求失败 (${res.status})`);
  return res.text();
}

// ── Mastery / Learner ───────────────────────────────────────

export async function listLearners(): Promise<string[]> {
  return apiGet<string[]>("/mastery/");
}

export async function fetchMasterySummary(
  learnerId: string,
): Promise<MasterySummary> {
  return apiGet<MasterySummary>(`/mastery/${learnerId}`);
}

export async function fetchWrongAnswers(
  learnerId: string,
  kpId?: string,
  limit = 10,
): Promise<WrongAnswer[]> {
  const params = new URLSearchParams();
  if (kpId) params.set("kp_id", kpId);
  params.set("limit", String(limit));
  return apiGet<WrongAnswer[]>(
    `/mastery/${learnerId}/wrong?${params.toString()}`,
  );
}

export async function fetchWeakPoints(
  learnerId: string,
): Promise<WeakPoint[]> {
  const data = await apiGet<{ weak_points: WeakPoint[] }>(
    `/mastery/${learnerId}/weak`,
  );
  return data.weak_points;
}

export async function fetchWeeklyStats(
  learnerId: string,
): Promise<PeriodStats> {
  return apiGet<PeriodStats>(`/mastery/${learnerId}/stats/weekly`);
}

export async function fetchMonthlyStats(
  learnerId: string,
): Promise<PeriodStats> {
  return apiGet<PeriodStats>(`/mastery/${learnerId}/stats/monthly`);
}

// ── Practice / Exam / Report ────────────────────────────────

export async function generatePractice(
  learnerId: string,
  kpId: string,
  count = 3,
): Promise<{ questions: PracticeQuestion[] }> {
  return apiPost("/practice/generate", {
    learner_id: learnerId,
    kp_id: kpId,
    count,
  });
}

export async function generateExam(
  learnerId: string,
): Promise<{
  exam_text: string;
  title: string;
  kp_covered: string[];
}> {
  return apiPost("/practice/exam", {
    learner_id: learnerId,
  });
}

export async function generateReport(
  learnerId: string,
  type: "daily" | "weekly" | "monthly",
): Promise<string> {
  return apiPostText("/report/generate", {
    learner_id: learnerId,
    type,
  });
}
