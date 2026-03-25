import { useState, useEffect, useRef, useCallback } from 'react'
import axios from 'axios'

// When deployed behind the same host (FastAPI serving React), this should stay empty
// so requests go to `/api/...`. Locally you can set `VITE_API_BASE_URL=http://localhost:8000`.
const API = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '')

/* ── Example chip ─────────────────────────────────────────────────────────── */
function ExampleChip({ question, onClick }) {
  const [hovered, setHovered] = useState(false)
  return (
    <button
      onClick={() => onClick(question)}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        background: hovered ? '#eff6ff' : '#ffffff',
        border: `1px solid ${hovered ? '#8db4e3' : '#dce6f2'}`,
        borderRadius: '8px',
        padding: '8px 12px',
        cursor: 'pointer',
        fontFamily: 'var(--font-mono)',
        fontSize: '11px',
        color: hovered ? '#23415f' : '#56657a',
        textAlign: 'left',
        lineHeight: 1.5,
        transition: 'all 0.15s ease',
        width: '100%',
      }}
    >
      {question}
    </button>
  )
}

/* ── Message bubble ───────────────────────────────────────────────────────── */
function Message({ role, content }) {
  const isUser = role === 'user'

  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      alignItems: isUser ? 'flex-end' : 'flex-start',
      gap: '4px',
      animation: 'fadeIn 0.2s ease both',
    }}>
      <div style={{
        fontSize: '9px',
        letterSpacing: '0.08em',
        textTransform: 'uppercase',
        color: 'var(--text-muted)',
        fontFamily: 'var(--font-mono)',
      }}>
        {isUser ? 'you' : 'assistant'}
      </div>
      <div style={{
        maxWidth: isUser ? '85%' : '100%',
        padding: '10px 14px',
        borderRadius: isUser
          ? '10px 10px 3px 10px'
          : '10px 10px 10px 3px',
        background: isUser
          ? '#1f2937'
          : '#ffffff',
        border: isUser
          ? 'none'
          : '1px solid #dce6f2',
        color: isUser ? '#f4f7fb' : '#1f2a37',
        fontFamily: 'var(--font-mono)',
        fontSize: '12.5px',
        lineHeight: 1.6,
        fontWeight: isUser ? 500 : 400,
        boxShadow: isUser ? 'none' : '0 2px 8px rgba(51,72,102,0.08)',
      }}>
        {content}
      </div>
    </div>
  )
}

/* ── Typing indicator ─────────────────────────────────────────────────────── */
function TypingIndicator() {
  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'flex-start',
      gap: '4px',
      animation: 'fadeIn 0.2s ease both',
    }}>
      <div style={{
        fontSize: '9px',
        letterSpacing: '0.08em',
        textTransform: 'uppercase',
        color: 'var(--text-muted)',
        fontFamily: 'var(--font-mono)',
      }}>
        assistant
      </div>
      <div style={{
        padding: '12px 16px',
        background: '#ffffff',
        border: '1px solid #dce6f2',
        borderRadius: '10px 10px 10px 3px',
        display: 'flex',
        alignItems: 'center',
        gap: '5px',
      }}>
        {[0, 1, 2].map(i => (
          <div key={i} style={{
            width: '5px',
            height: '5px',
            background: '#8ba0b7',
            borderRadius: '50%',
            animation: `blink 1.2s ease-in-out ${i * 0.2}s infinite`,
          }} />
        ))}
      </div>
    </div>
  )
}

/* ── Empty state ──────────────────────────────────────────────────────────── */
function EmptyState({ examples, onSelect, loading }) {
  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      height: '100%',
      justifyContent: 'center',
      gap: '20px',
      padding: '8px',
    }}>
      {/* Branding mark */}
      <div style={{ textAlign: 'center' }}>
        <div style={{
          fontFamily: 'var(--font-display)',
          fontSize: '28px',
          fontWeight: 800,
          letterSpacing: '-0.03em',
          color: '#2f80ed',
          lineHeight: 1,
        }}>
          O2C
        </div>
        <div style={{
          fontFamily: 'var(--font-display)',
          fontSize: '11px',
          fontWeight: 600,
          letterSpacing: '0.18em',
          textTransform: 'uppercase',
          color: '#8a96a6',
          marginTop: '6px',
        }}>
          Data Explorer
        </div>
        <div style={{
          fontSize: '11px',
          color: '#5f6f84',
          marginTop: '10px',
          lineHeight: 1.6,
        }}>
          Ask anything about your order-to-cash data.
          <br />
          Powered by O2C AI.
        </div>
      </div>

      {/* Examples */}
      {loading ? (
        <div style={{
          display: 'flex', justifyContent: 'center', gap: '6px',
          color: 'var(--text-muted)', fontSize: '11px', alignItems: 'center',
        }}>
          <div style={{
            width: '12px', height: '12px',
            border: '1.5px solid #d5e1ef',
            borderTopColor: '#2f80ed',
            borderRadius: '50%',
            animation: 'spin 0.8s linear infinite',
          }} />
          Loading examples…
        </div>
      ) : examples.length > 0 ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
          <div style={{
            fontSize: '9px',
            letterSpacing: '0.12em',
            textTransform: 'uppercase',
            color: '#8a96a6',
            marginBottom: '4px',
            fontFamily: 'var(--font-mono)',
          }}>
            Try asking
          </div>
          {examples.map((ex, i) => (
            <ExampleChip key={i} question={ex.question} onClick={onSelect} />
          ))}
        </div>
      ) : null}
    </div>
  )
}

/* ── Main Chat component ───────────────────────────────────────────────────── */
export default function Chat(props) {
  const [messages, setMessages]     = useState([])
  const [input, setInput]           = useState('')
  const [loading, setLoading]       = useState(false)
  const [examples, setExamples]     = useState([])
  const [exLoading, setExLoading]   = useState(true)
  // Session id is generated per page lifetime (no localStorage) so
  // conversation memory resets on refresh.
  const [sessionId] = useState(() => {
    try {
      if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID()
    } catch (e) {}
    return `sess_${Math.random().toString(36).slice(2)}_${Date.now()}`
  })
  const bottomRef                   = useRef(null)
  const textareaRef                 = useRef(null)

  /* fetch examples once on mount */
  useEffect(() => {
    axios.get(`${API}/api/examples`)
      .then(({ data }) => setExamples(data.examples || []))
      .catch(() => {})
      .finally(() => setExLoading(false))
  }, [])

  /* auto-scroll to latest message */
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  /* auto-resize textarea */
  const handleInputChange = (e) => {
    setInput(e.target.value)
    const ta = e.target
    ta.style.height = 'auto'
    ta.style.height = Math.min(ta.scrollHeight, 120) + 'px'
  }

  const sendMessage = useCallback(async (text) => {
    const question = (text || input).trim()
    if (!question || loading) return

    setMessages(prev => [...prev, { role: 'user', content: question }])
    setInput('')
    if (textareaRef.current) {
      textareaRef.current.style.height = '42px'
    }
    setLoading(true)

    try {
      const { data } = await axios.post(`${API}/api/chat`, {
        message: question,
        session_id: sessionId,
      })
      setMessages(prev => [...prev, { role: 'assistant', content: data.answer }])
      props.onAnswer?.(data.answer)
    } catch (err) {
      const msg = err.response?.data?.detail || 'Something went wrong. Is the backend running?'
      setMessages(prev => [...prev, { role: 'assistant', content: `⚠ ${msg}` }])
    } finally {
      setLoading(false)
    }
  }, [input, loading, sessionId])

  /* send on Enter (Shift+Enter = newline) */
  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  const isEmpty = messages.length === 0

  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      height: '100%',
      background: '#ffffff',
    }}>
      <div style={{
        padding: '14px 14px 10px',
        borderBottom: '1px solid #e0e8f3',
        flexShrink: 0,
      }}>
        <div style={{ fontSize: '12px', color: '#1e2a39', fontWeight: 700 }}>
          Chat with Graph
        </div>
        <div style={{ fontSize: '10px', color: '#8a96a6', marginTop: '1px' }}>
          Order to Cash
        </div>
        <div style={{
          marginTop: '10px',
          display: 'flex',
          gap: '10px',
          alignItems: 'center',
          padding: '10px 10px',
          borderRadius: '10px',
          border: '1px solid #e0e8f3',
          background: '#fcfdff',
        }}>
          <div style={{
            width: '26px',
            height: '26px',
            borderRadius: '50%',
            background: '#0f172a',
            color: '#ffffff',
            display: 'grid',
            placeItems: 'center',
            fontSize: '12px',
            fontWeight: 700,
          }}>
            B
          </div>
          <div>
            <div style={{ fontSize: '12px', color: '#1f2a37', fontWeight: 700 }}>O2C AI</div>
            <div style={{ fontSize: '10px', color: '#8a96a6' }}>Graph Agent</div>
          </div>
        </div>
      </div>

      <div style={{
        flex: 1,
        overflowY: 'auto',
        padding: isEmpty ? '0 16px' : '16px',
        display: 'flex',
        flexDirection: 'column',
        gap: '12px',
      }}>
        {isEmpty ? (
          <EmptyState
            examples={examples}
            onSelect={sendMessage}
            loading={exLoading}
          />
        ) : (
          <>
            {messages.map((msg, i) => (
              <Message key={i} role={msg.role} content={msg.content} />
            ))}
            {loading && <TypingIndicator />}
          </>
        )}
        <div ref={bottomRef} />
      </div>

      {/* ── Input area ── */}
      <div style={{
        padding: '10px 16px 14px',
        borderTop: '1px solid #e0e8f3',
        flexShrink: 0,
        background: '#fafcff',
      }}>
        <div style={{
          marginBottom: '8px',
          fontSize: '10px',
          color: '#7f8b99',
          display: 'flex',
          alignItems: 'center',
          gap: '5px',
        }}>
          <span style={{
            width: '6px',
            height: '6px',
            borderRadius: '50%',
            background: '#26c281',
            boxShadow: '0 0 0 3px rgba(38,194,129,0.12)',
          }} />
          O2C AI is awaiting instructions
        </div>
        <div style={{ display: 'flex', gap: '8px', alignItems: 'flex-end' }}>
          <textarea
            ref={textareaRef}
            value={input}
            onChange={handleInputChange}
            onKeyDown={handleKeyDown}
            placeholder="Ask about orders, products, billing…"
            rows={1}
            disabled={loading}
            style={{
              flex: 1,
              background: '#ffffff',
              border: `1px solid ${input ? '#adc5df' : '#dce6f2'}`,
              borderRadius: '10px',
              padding: '10px 14px',
              color: '#1f2a37',
              fontFamily: 'var(--font-mono)',
              fontSize: '12.5px',
              lineHeight: 1.5,
              resize: 'none',
              height: '42px',
              maxHeight: '120px',
              outline: 'none',
              transition: 'border-color 0.15s',
            }}
            onFocus={e => e.target.style.borderColor = '#2f80ed'}
            onBlur={e => e.target.style.borderColor = input ? '#adc5df' : '#dce6f2'}
          />
          <button
            onClick={() => sendMessage()}
            disabled={!input.trim() || loading}
            style={{
              width: '42px',
              height: '42px',
              borderRadius: '10px',
              background: (!input.trim() || loading) ? '#eef3f9' : '#2f80ed',
              border: `1px solid ${(!input.trim() || loading) ? '#dce6f2' : '#2f80ed'}`,
              cursor: (!input.trim() || loading) ? 'not-allowed' : 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              flexShrink: 0,
              transition: 'all 0.15s ease',
              color: (!input.trim() || loading) ? '#9aa7b8' : '#ffffff',
            }}
          >
            {loading ? (
              <div style={{
                width: '14px',
                height: '14px',
                border: '1.5px solid #aac4df',
                borderTopColor: '#2f80ed',
                borderRadius: '50%',
                animation: 'spin 0.8s linear infinite',
              }} />
            ) : (
              <svg width="15" height="15" viewBox="0 0 15 15" fill="none">
                <path
                  d="M1 7.5h13M8.5 2L14 7.5 8.5 13"
                  stroke="currentColor"
                  strokeWidth="1.5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            )}
          </button>
        </div>
        <div style={{
          marginTop: '7px',
          fontSize: '10px',
          color: '#8a96a6',
          textAlign: 'right',
          fontFamily: 'var(--font-mono)',
        }}>
          Enter to send ·
        </div>
      </div>
    </div>
  )
}
