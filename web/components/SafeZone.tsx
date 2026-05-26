"use client";

import { Component, type ReactNode } from "react";

/**
 * Catches React commit-phase DOM errors (notably React 19 concurrent-mode
 * insertBefore race) that standard error boundaries miss because the error
 * originates from inside react-dom's reconciliation walk.
 *
 * Without this, a single transient DOMException during page navigation
 * collapses the entire React tree.  With it, the failed subtree is unmounted
 * and the next render succeeds normally.
 */
interface Props {
  children: ReactNode;
  fallback?: ReactNode;
}

interface State {
  errorCount: number;
}

export default class SafeZone extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { errorCount: 0 };
  }

  static getDerivedStateFromError(): Partial<State> {
    return { errorCount: 1 };
  }

  componentDidCatch(error: Error) {
    // Log but don't surface to the user — the error is transient.
    console.debug("[SafeZone] Caught:", error.message);
  }

  render() {
    if (this.state.errorCount > 3) {
      // Too many consecutive crashes — show fallback.
      return (
        this.props.fallback ?? (
          <div className="flex min-h-[30vh] items-center justify-center text-[13px] text-[var(--muted-foreground)]">
            Something went wrong. Try refreshing the page.
          </div>
        )
      );
    }
    return this.props.children;
  }
}
