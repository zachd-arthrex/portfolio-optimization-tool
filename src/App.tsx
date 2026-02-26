import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { ChangeEvent, KeyboardEvent } from 'react'
import './App.css'

type TabKey = 'project-data' | 'scheduler'

const DEFAULT_DISCIPLINES = ['PjM', 'SE', 'ME', 'EE', 'FW', 'SW', 'CV', 'Test']
const DISCIPLINE_PALETTE = [
  '#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f',
  '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac',
  '#86bcb6', '#8cd17d', '#b6992d', '#499894', '#d37295',
  '#f1ce63', '#a0cbe8', '#ffbe7d', '#fab0e4', '#79706e',
]
const getDisciplineColor = (disciplines: string[], discipline: string): string => {
  const idx = disciplines.indexOf(discipline)
  return DISCIPLINE_PALETTE[idx >= 0 ? idx % DISCIPLINE_PALETTE.length : 0] ?? '#999'
}
const STORAGE_KEY = 'portfolio-planning-project-data-v2'
const MAX_UNDO_STEPS = 50

interface LineItem {
  uid: number
  name: string
  parentProjectUid: number | null
  dependencyUids: number[]
  valueScore: number | null
  durationMonths: number | null
  resourceAllocation: Record<string, number | null>
}

interface SchedulerOverride {
  startMonth: number
  frozen: boolean
}

interface PersistedState {
  nextId: number
  lineItems: LineItem[]
  collapsedProjectIds: number[]
  disciplines: string[]
  teamCapacities: Record<string, number | null>
  schedulerOverrides: Record<number, SchedulerOverride>
}

interface Task {
  id: number
  name: string
  projectId: number
  projectName: string
  priority: number
  duration: number
  dependencyIds: number[]
  requirements: Record<string, number>
  order: number
  isStandaloneProject: boolean
}

interface ScheduledTask extends Task {
  startMonth: number
  endMonth: number
  frozen: boolean
}

const emptyAllocation = (disciplines: string[]): Record<string, number | null> =>
  Object.fromEntries(disciplines.map((d) => [d, null]))

const defaultCapacities = (disciplines: string[]): Record<string, number | null> =>
  Object.fromEntries(disciplines.map((d) => [d, 1]))

const parseOptionalNumber = (rawValue: string): number | null => {
  if (rawValue.trim() === '') {
    return null
  }
  const parsed = Number(rawValue)
  if (!Number.isFinite(parsed) || parsed < 0) {
    return null
  }
  return parsed
}

const toNumberInputValue = (value: number | null) => (value === null ? '' : value)
const numberOrZero = (value: number | null) => value ?? 0
const isProjectRow = (item: LineItem) => item.parentProjectUid === null

const normalizeHierarchyByOrder = (rows: LineItem[]): LineItem[] => {
  let nearestProjectId: number | null = null

  return rows.map((row) => {
    if (row.parentProjectUid === null) {
      nearestProjectId = row.uid
      return row
    }

    return {
      ...row,
      parentProjectUid: nearestProjectId,
      valueScore: null,
    }
  })
}

const cloneOverrides = (overrides: Record<number, SchedulerOverride>): Record<number, SchedulerOverride> => {
  const clone: Record<number, SchedulerOverride> = {}
  Object.entries(overrides).forEach(([key, value]) => {
    clone[Number(key)] = { ...value }
  })
  return clone
}

// --- URL share helpers (deflate-raw → base64url) ---
const compressToBase64 = async (json: string): Promise<string> => {
  const bytes = new TextEncoder().encode(json)
  const cs = new CompressionStream('deflate-raw')
  const writer = cs.writable.getWriter()
  writer.write(bytes)
  writer.close()
  const chunks: Uint8Array[] = []
  const reader = cs.readable.getReader()
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    chunks.push(value)
  }
  const totalLength = chunks.reduce((s, c) => s + c.length, 0)
  const merged = new Uint8Array(totalLength)
  let offset = 0
  for (const c of chunks) { merged.set(c, offset); offset += c.length }
  let binary = ''
  merged.forEach((b) => { binary += String.fromCharCode(b) })
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

const decompressFromBase64 = async (encoded: string): Promise<string> => {
  const padded = encoded.replace(/-/g, '+').replace(/_/g, '/') + '=='.slice(0, (4 - (encoded.length % 4)) % 4)
  const binary = atob(padded)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i)
  const ds = new DecompressionStream('deflate-raw')
  const writer = ds.writable.getWriter()
  writer.write(bytes)
  writer.close()
  const chunks: Uint8Array[] = []
  const reader = ds.readable.getReader()
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    chunks.push(value)
  }
  const totalLength = chunks.reduce((s, c) => s + c.length, 0)
  const merged = new Uint8Array(totalLength)
  let off = 0
  for (const c of chunks) { merged.set(c, off); off += c.length }
  return new TextDecoder().decode(merged)
}

const getSavedState = (): PersistedState | null => {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) {
      return null
    }

    const parsed = JSON.parse(raw) as Partial<PersistedState>
    if (!Array.isArray(parsed.lineItems) || typeof parsed.nextId !== 'number') {
      return null
    }

    const disciplines = Array.isArray(parsed.disciplines) ? parsed.disciplines : DEFAULT_DISCIPLINES

    return {
      nextId: Math.max(1, parsed.nextId),
      lineItems: parsed.lineItems,
      collapsedProjectIds: Array.isArray(parsed.collapsedProjectIds) ? parsed.collapsedProjectIds : [],
      disciplines,
      teamCapacities: parsed.teamCapacities ?? defaultCapacities(disciplines),
      schedulerOverrides: parsed.schedulerOverrides ?? {},
    }
  } catch {
    return null
  }
}

const calculateProjectDuration = (rows: LineItem[], projectUid: number): number => {
  const phases = rows.filter((item) => item.parentProjectUid === projectUid)
  if (phases.length === 0) {
    return 0
  }

  const phaseUidSet = new Set(phases.map((phase) => phase.uid))
  const phaseDurationMap = new Map(phases.map((phase) => [phase.uid, Math.max(1, numberOrZero(phase.durationMonths))]))
  const dependencyMap = new Map<number, number[]>(
    phases.map((phase) => [
      phase.uid,
      phase.dependencyUids.filter((dependencyUid) => phaseUidSet.has(dependencyUid)),
    ]),
  )

  const memoizedFinish = new Map<number, number>()
  const visiting = new Set<number>()

  const getFinish = (phaseUid: number): number => {
    if (memoizedFinish.has(phaseUid)) {
      return memoizedFinish.get(phaseUid) ?? 0
    }

    if (visiting.has(phaseUid)) {
      return phaseDurationMap.get(phaseUid) ?? 1
    }

    visiting.add(phaseUid)
    const dependencies = dependencyMap.get(phaseUid) ?? []
    const earliestStart = dependencies.reduce(
      (maxFinish, dependencyUid) => Math.max(maxFinish, getFinish(dependencyUid)),
      0,
    )
    const finish = earliestStart + (phaseDurationMap.get(phaseUid) ?? 1)
    visiting.delete(phaseUid)
    memoizedFinish.set(phaseUid, finish)
    return finish
  }

  return phases.reduce((maxFinish, phase) => Math.max(maxFinish, getFinish(phase.uid)), 0)
}

function App() {
  const savedState = getSavedState()

  const [activeTab, setActiveTab] = useState<TabKey>('project-data')
  const [nextId, setNextId] = useState(savedState?.nextId ?? 1)
  const [lineItems, setLineItems] = useState<LineItem[]>(savedState?.lineItems ?? [])
  const [collapsedProjects, setCollapsedProjects] = useState<Set<number>>(
    new Set(savedState?.collapsedProjectIds ?? []),
  )
  const [disciplines, setDisciplines] = useState<string[]>(
    savedState?.disciplines ?? DEFAULT_DISCIPLINES,
  )
  const [teamCapacities, setTeamCapacities] = useState<Record<string, number | null>>(
    savedState?.teamCapacities ?? defaultCapacities(DEFAULT_DISCIPLINES),
  )
  const [schedulerOverrides, setSchedulerOverrides] = useState<Record<number, SchedulerOverride>>(
    savedState?.schedulerOverrides ?? {},
  )

  // Load state from URL hash on first mount (async decompression)
  const urlLoadedRef = useRef(false)
  useEffect(() => {
    if (urlLoadedRef.current) return
    urlLoadedRef.current = true
    const hash = window.location.hash.slice(1)
    if (!hash.startsWith('data=')) return
    const encoded = hash.slice(5)
    decompressFromBase64(encoded)
      .then((json) => {
        const parsed = JSON.parse(json) as PersistedState
        if (!Array.isArray(parsed.lineItems)) return
        const loadedDisciplines = Array.isArray(parsed.disciplines) ? parsed.disciplines : DEFAULT_DISCIPLINES
        setNextId(Math.max(1, parsed.nextId ?? 1))
        setLineItems(parsed.lineItems)
        setCollapsedProjects(new Set(parsed.collapsedProjectIds ?? []))
        setDisciplines(loadedDisciplines)
        setTeamCapacities(parsed.teamCapacities ?? defaultCapacities(loadedDisciplines))
        setSchedulerOverrides(parsed.schedulerOverrides ?? {})
        setStatusMessage('Loaded project data from shared link.')
        // Clear hash so refreshing uses localStorage going forward
        window.history.replaceState(null, '', window.location.pathname + window.location.search)
      })
      .catch(() => { /* ignore bad hash */ })
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const [statusMessage, setStatusMessage] = useState('')
  const [undoStack, setUndoStack] = useState<PersistedState[]>([])
  const [dependencyDrafts, setDependencyDrafts] = useState<Record<number, string>>({})
  const [schedulerUndoStack, setSchedulerUndoStack] = useState<Record<number, SchedulerOverride>[]>([])
  const [schedulerRedoStack, setSchedulerRedoStack] = useState<Record<number, SchedulerOverride>[]>([])

  const [pixelsPerMonth, setPixelsPerMonth] = useState(36)
  const [collapsedProjectsInScheduler, setCollapsedProjectsInScheduler] = useState<Set<number>>(new Set())
  const [histogramMode, setHistogramMode] = useState<'separate' | 'combined'>('separate')
  const [timeGranularity, setTimeGranularity] = useState<'auto' | 'monthly' | 'quarterly'>('auto')
  const [dragTaskId, setDragTaskId] = useState<number | null>(null)
  const [dragStartX, setDragStartX] = useState(0)
  const [dragInitialMonth, setDragInitialMonth] = useState(0)
  const [dragBaseTasks, setDragBaseTasks] = useState<ScheduledTask[] | null>(null)
  const [dragBaseOverrides, setDragBaseOverrides] = useState<Record<number, SchedulerOverride> | null>(null)
  const [dragPreviewOverrides, setDragPreviewOverrides] = useState<Record<number, SchedulerOverride> | null>(null)

  const loadFileInputRef = useRef<HTMLInputElement | null>(null)
  const nameInputRefs = useRef<Record<number, HTMLInputElement | null>>({})
  const [pendingFocusRowId, setPendingFocusRowId] = useState<number | null>(null)
  const [rowDragUid, setRowDragUid] = useState<number | null>(null)
  const [rowDropTargetUid, setRowDropTargetUid] = useState<number | null>(null)

  // Column widths for the Project Data table (resizable)
  const COL_KEYS = useMemo(
    () => ['actions', 'id', 'name', 'deps', 'value', 'duration', ...disciplines.map((t) => `fte-${t}`)],
    [disciplines],
  )
  const COL_WIDTHS_KEY = 'portfolio-planning-col-widths'
  const [colWidths, setColWidths] = useState<Record<string, number>>(() => {
    try {
      const saved = localStorage.getItem(COL_WIDTHS_KEY)
      if (saved) {
        const parsed = JSON.parse(saved)
        return { ...Object.fromEntries(DEFAULT_DISCIPLINES.map((t) => [`fte-${t}`, 85])), actions: 76, id: 91, name: 533, deps: 80, value: 81, duration: 100, ...parsed }
      }
    } catch { /* ignore */ }
    return { actions: 76, id: 91, name: 533, deps: 80, value: 81, duration: 100, ...Object.fromEntries(DEFAULT_DISCIPLINES.map((t) => [`fte-${t}`, 85])) }
  })

  // Ensure col widths exist for new disciplines
  const effectiveColWidths = useMemo(() => {
    const result = { ...colWidths }
    for (const d of disciplines) {
      if (result[`fte-${d}`] === undefined) result[`fte-${d}`] = 85
    }
    return result
  }, [colWidths, disciplines])

  const handleColResizeStart = (col: string, event: React.MouseEvent) => {
    event.preventDefault()
    event.stopPropagation()
    const startX = event.clientX
    const startW = effectiveColWidths[col] ?? 85
    const onMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientX - startX
      const newWidth = Math.max(30, startW + delta)
      setColWidths((prev) => {
        const next = { ...prev, [col]: newWidth }
        localStorage.setItem(COL_WIDTHS_KEY, JSON.stringify(next))
        return next
      })
    }
    const onUp = () => {
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
    }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }

  const tableRef = useRef<HTMLTableElement | null>(null)


  const handleTableArrowNav = (event: React.KeyboardEvent<HTMLTableElement>) => {
    const { key } = event
    if (key !== 'ArrowUp' && key !== 'ArrowDown' && key !== 'ArrowLeft' && key !== 'ArrowRight') return
    const target = event.target as HTMLElement
    if (target.tagName !== 'INPUT') return

    // For text inputs, only navigate left/right when cursor is at the edge
    const input = target as HTMLInputElement
    if (key === 'ArrowLeft' && input.type !== 'number' && (input.selectionStart ?? 0) > 0) return
    if (key === 'ArrowRight' && input.type !== 'number' && (input.selectionStart ?? 0) < input.value.length) return

    const table = tableRef.current
    if (!table) return

    // Collect all visible inputs in the tbody in DOM order
    const tbody = table.querySelector('tbody')
    if (!tbody) return
    const rows = Array.from(tbody.querySelectorAll('tr'))
    const grid: HTMLInputElement[][] = []
    for (const row of rows) {
      const inputs = Array.from(row.querySelectorAll('input')) as HTMLInputElement[]
      if (inputs.length > 0) grid.push(inputs)
    }

    // Find current position
    let rowIdx = -1
    let colIdx = -1
    for (let r = 0; r < grid.length; r++) {
      const c = grid[r].indexOf(input)
      if (c !== -1) {
        rowIdx = r
        colIdx = c
        break
      }
    }
    if (rowIdx === -1) return

    let nextRow = rowIdx
    let nextCol = colIdx
    if (key === 'ArrowUp') nextRow = Math.max(0, rowIdx - 1)
    if (key === 'ArrowDown') nextRow = Math.min(grid.length - 1, rowIdx + 1)
    if (key === 'ArrowLeft') nextCol = Math.max(0, colIdx - 1)
    if (key === 'ArrowRight') nextCol = Math.min(grid[rowIdx].length - 1, colIdx + 1)

    // Clamp column if target row has fewer inputs
    nextCol = Math.min(nextCol, grid[nextRow].length - 1)

    if (nextRow !== rowIdx || nextCol !== colIdx) {
      event.preventDefault()
      const nextInput = grid[nextRow][nextCol]
      nextInput.focus()
      nextInput.select()
    }
  }

  const timelineStart = useMemo(() => {
    const now = new Date()
    return new Date(now.getFullYear(), now.getMonth(), 1)
  }, [])

  const createSnapshot = (): PersistedState => ({
    nextId,
    lineItems: lineItems.map((item) => ({
      ...item,
      dependencyUids: [...item.dependencyUids],
      resourceAllocation: { ...item.resourceAllocation },
    })),
    collapsedProjectIds: Array.from(collapsedProjects),
    disciplines: [...disciplines],
    teamCapacities: { ...teamCapacities },
    schedulerOverrides: cloneOverrides(schedulerOverrides),
  })

  const rememberForUndo = () => {
    const snapshot = createSnapshot()
    setUndoStack((previous) => {
      const next = [...previous, snapshot]
      return next.length > MAX_UNDO_STEPS ? next.slice(next.length - MAX_UNDO_STEPS) : next
    })
  }

  const persistToLocalStorage = () => {
    const payload: PersistedState = {
      nextId,
      lineItems,
      collapsedProjectIds: Array.from(collapsedProjects),
      disciplines,
      teamCapacities,
      schedulerOverrides,
    }
    localStorage.setItem(STORAGE_KEY, JSON.stringify(payload))
  }

  const createBlankLineItem = (uid: number, parentProjectUid: number | null): LineItem => ({
    uid,
    name: '',
    parentProjectUid,
    dependencyUids: [],
    valueScore: null,
    durationMonths: null,
    resourceAllocation: emptyAllocation(disciplines),
  })

  const loadFromPayload = (payload: PersistedState) => {
    const loadedDisciplines = Array.isArray(payload.disciplines) ? payload.disciplines : DEFAULT_DISCIPLINES

    const normalizedItems = payload.lineItems.map((item) => ({
      uid: Number(item.uid),
      name: typeof item.name === 'string' ? item.name : '',
      parentProjectUid:
        item.parentProjectUid === null || item.parentProjectUid === undefined
          ? null
          : Number(item.parentProjectUid),
      dependencyUids: Array.isArray(item.dependencyUids)
        ? item.dependencyUids
            .map((value) => Number(value))
            .filter((value) => Number.isFinite(value) && value > 0)
        : [],
      valueScore:
        item.valueScore === null || item.valueScore === undefined
          ? null
          : Number.isFinite(Number(item.valueScore))
            ? Number(item.valueScore)
            : null,
      durationMonths:
        item.durationMonths === null || item.durationMonths === undefined
          ? null
          : Number.isFinite(Number(item.durationMonths))
            ? Number(item.durationMonths)
            : null,
      resourceAllocation: loadedDisciplines.reduce<Record<string, number | null>>((acc, team) => {
        const raw = item.resourceAllocation?.[team]
        acc[team] =
          raw === null || raw === undefined ? null : Number.isFinite(Number(raw)) ? Number(raw) : null
        return acc
      }, {} as Record<string, number | null>),
    }))

    const maxId = normalizedItems.reduce((max, row) => Math.max(max, row.uid), 0)
    setLineItems(normalizedItems)
    setNextId(Math.max(maxId + 1, Number(payload.nextId) || 1))
    setCollapsedProjects(
      new Set(
        Array.isArray(payload.collapsedProjectIds)
          ? payload.collapsedProjectIds.map((id) => Number(id)).filter((id) => Number.isFinite(id))
          : [],
      ),
    )

    setDisciplines(loadedDisciplines)

    const normalizedCapacity = defaultCapacities(loadedDisciplines)
    loadedDisciplines.forEach((team) => {
      const raw = payload.teamCapacities?.[team]
      normalizedCapacity[team] =
        raw === null || raw === undefined ? null : Number.isFinite(Number(raw)) ? Number(raw) : null
    })
    setTeamCapacities(normalizedCapacity)

    const normalizedOverrides: Record<number, SchedulerOverride> = {}
    Object.entries(payload.schedulerOverrides ?? {}).forEach(([key, value]) => {
      const id = Number(key)
      if (Number.isFinite(id) && typeof value?.startMonth === 'number') {
        normalizedOverrides[id] = {
          startMonth: Math.max(0, Math.floor(value.startMonth)),
          frozen: Boolean(value.frozen),
        }
      }
    })
    setSchedulerOverrides(normalizedOverrides)
    setSchedulerUndoStack([])
    setSchedulerRedoStack([])
    setDependencyDrafts({})
  }

  const pushSchedulerHistory = (current: Record<number, SchedulerOverride>) => {
    setSchedulerUndoStack((previous) => [...previous, cloneOverrides(current)])
    setSchedulerRedoStack([])
  }

  const addProject = () => {
    rememberForUndo()
    const newLineItem = createBlankLineItem(nextId, null)
    setLineItems((previous) => [...previous, newLineItem])
    setNextId((previous) => previous + 1)
  }

  const updateLineItem = <K extends keyof LineItem>(uid: number, key: K, value: LineItem[K]) => {
    rememberForUndo()
    setLineItems((previous) =>
      previous.map((item) => (item.uid === uid ? { ...item, [key]: value } : item)),
    )
  }

  const updateAllocation = (uid: number, team: string, value: number | null) => {
    rememberForUndo()
    setLineItems((previous) =>
      previous.map((item) =>
        item.uid === uid
          ? {
              ...item,
              resourceAllocation: {
                ...item.resourceAllocation,
                [team]: value,
              },
            }
          : item,
      ),
    )
  }

  const removeRow = (uid: number) => {
    rememberForUndo()
    setLineItems((previous) => {
      const removedUids = new Set<number>([uid])
      previous.forEach((item) => {
        if (item.parentProjectUid === uid) {
          removedUids.add(item.uid)
        }
      })

      return previous
        .filter((item) => !removedUids.has(item.uid))
        .map((item) => ({
          ...item,
          dependencyUids: item.dependencyUids.filter((dependencyUid) => !removedUids.has(dependencyUid)),
        }))
    })

    setCollapsedProjects((previous) => {
      const next = new Set(previous)
      lineItems.forEach((item) => {
        if (item.uid === uid || item.parentProjectUid === uid) {
          next.delete(item.uid)
        }
      })
      return next
    })

    setSchedulerOverrides((previous) => {
      const next = { ...previous }
      delete next[uid]
      return next
    })
  }

  const toggleProjectCollapsed = (projectUid: number) => {
    setCollapsedProjects((previous) => {
      const next = new Set(previous)
      if (next.has(projectUid)) {
        next.delete(projectUid)
      } else {
        next.add(projectUid)
      }
      return next
    })
  }

  const hasPreviousProject = (uid: number): boolean => {
    const rowIndex = lineItems.findIndex((row) => row.uid === uid)
    if (rowIndex <= 0) {
      return false
    }
    return lineItems.slice(0, rowIndex).some((candidate) => isProjectRow(candidate))
  }

  const canIndent = (uid: number): boolean => {
    const row = lineItems.find((item) => item.uid === uid)
    if (!row) {
      return false
    }
    return hasPreviousProject(uid)
  }

  const canOutdent = (uid: number): boolean => {
    const row = lineItems.find((item) => item.uid === uid)
    return !!row && row.parentProjectUid !== null
  }

  const indentRow = (uid: number) => {
    if (!canIndent(uid)) {
      return
    }

    rememberForUndo()
    setLineItems((previous) => {
      const targetIndex = previous.findIndex((item) => item.uid === uid)
      if (targetIndex <= 0) {
        return previous
      }

      let previousProjectUid: number | null = null
      for (let index = targetIndex - 1; index >= 0; index -= 1) {
        if (isProjectRow(previous[index])) {
          previousProjectUid = previous[index].uid
          break
        }
      }

      if (!previousProjectUid) {
        return previous
      }

      const updated = previous.map((item) =>
        item.uid === uid
          ? {
              ...item,
              parentProjectUid: previousProjectUid,
              valueScore: null,
            }
          : item,
      )

      return normalizeHierarchyByOrder(updated)
    })
  }

  const outdentRow = (uid: number) => {
    if (!canOutdent(uid)) {
      return
    }

    rememberForUndo()
    setLineItems((previous) => {
      const updated = previous.map((item) =>
        item.uid === uid
          ? {
              ...item,
              parentProjectUid: null,
            }
          : item,
      )

      return normalizeHierarchyByOrder(updated)
    })
  }

  const addRowBelow = (uid: number) => {
    rememberForUndo()
    const currentIndex = lineItems.findIndex((row) => row.uid === uid)
    if (currentIndex < 0) {
      return
    }

    const currentRow = lineItems[currentIndex]
    const insertAt = currentIndex + 1
    const newRowId = nextId
    const newLineItem = createBlankLineItem(newRowId, currentRow.parentProjectUid)

    setLineItems((previous) =>
      normalizeHierarchyByOrder([
        ...previous.slice(0, insertAt),
        newLineItem,
        ...previous.slice(insertAt),
      ]),
    )
    setNextId((previous) => previous + 1)
    setPendingFocusRowId(newRowId)
  }

  const handleHierarchyKeyDown = (event: KeyboardEvent<HTMLInputElement>, item: LineItem) => {
    if (event.key === 'Enter') {
      event.preventDefault()
      addRowBelow(item.uid)
      return
    }

    if (event.key !== 'Tab') {
      return
    }

    event.preventDefault()

    if (event.shiftKey) {
      if (canOutdent(item.uid)) {
        outdentRow(item.uid)
      } else {
        setStatusMessage('This row cannot be outdented further.')
      }
      return
    }

    if (canIndent(item.uid)) {
      indentRow(item.uid)
    } else {
      setStatusMessage('This row cannot be indented in the current structure.')
    }
  }

  // --- Row drag-and-drop reordering (Project Data tab) ---
  const getRowGroup = (uid: number): number[] => {
    // Returns the uid followed by all its child phase uids (in order)
    const item = lineItems.find((li) => li.uid === uid)
    if (!item) return []
    if (item.parentProjectUid !== null) {
      // It's a phase — just itself
      return [uid]
    }
    // It's a project — include all child phases
    const children = lineItems.filter((li) => li.parentProjectUid === uid)
    return [uid, ...children.map((c) => c.uid)]
  }

  const handleRowDragStart = (event: React.DragEvent<HTMLTableRowElement>, uid: number) => {
    const item = lineItems.find((li) => li.uid === uid)
    if (!item) return
    // Dragging a project drags the whole group; dragging a phase drags just the phase
    setRowDragUid(uid)
    event.dataTransfer.effectAllowed = 'move'
    event.dataTransfer.setData('text/plain', String(uid))
  }

  const handleRowDragOver = (event: React.DragEvent<HTMLTableRowElement>, uid: number) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = 'move'
    if (uid === rowDragUid) return
    // When dragging a project, only show drop indicator on other project rows
    const dragItem = rowDragUid !== null ? lineItems.find((li) => li.uid === rowDragUid) : null
    if (dragItem && dragItem.parentProjectUid === null) {
      const targetItem = lineItems.find((li) => li.uid === uid)
      if (targetItem && targetItem.parentProjectUid !== null) {
        setRowDropTargetUid(null)
        return
      }
    }
    setRowDropTargetUid(uid)
  }

  const handleRowDrop = (event: React.DragEvent<HTMLTableRowElement>, targetUid: number) => {
    event.preventDefault()
    if (rowDragUid === null) return
    if (targetUid === rowDragUid) {
      setRowDragUid(null)
      setRowDropTargetUid(null)
      return
    }

    const dragItem = lineItems.find((li) => li.uid === rowDragUid)
    const targetItem = lineItems.find((li) => li.uid === targetUid)
    if (!dragItem || !targetItem) {
      setRowDragUid(null)
      setRowDropTargetUid(null)
      return
    }

    rememberForUndo()

    const isDragProject = dragItem.parentProjectUid === null
    const isTargetProject = targetItem.parentProjectUid === null

    if (isDragProject) {
      // --- Dragging a project (with all its phases) ---
      const dragGroup = getRowGroup(rowDragUid)
      const remaining = lineItems.filter((li) => !dragGroup.includes(li.uid))
      const itemsToMove = dragGroup.map((id) => lineItems.find((li) => li.uid === id)!).filter(Boolean)

      // Determine insertion point: before the target project group
      const insertBeforeUid = isTargetProject ? targetUid : targetItem.parentProjectUid!
      const targetIndex = remaining.findIndex((li) => li.uid === insertBeforeUid)
      if (targetIndex === -1) {
        setRowDragUid(null)
        setRowDropTargetUid(null)
        return
      }
      const newItems = [...remaining]
      newItems.splice(targetIndex, 0, ...itemsToMove)
      setLineItems(newItems)
    } else {
      // --- Dragging a single phase ---
      const remaining = lineItems.filter((li) => li.uid !== rowDragUid)
      const movedPhase = { ...dragItem }

      if (isTargetProject) {
        // Dropping on a project header → reparent phase to that project,
        // insert at the end of that project's children
        movedPhase.parentProjectUid = targetUid
        const lastChildIndex = remaining.reduce(
          (last, li, idx) => (li.parentProjectUid === targetUid ? idx : last),
          remaining.findIndex((li) => li.uid === targetUid),
        )
        const newItems = [...remaining]
        newItems.splice(lastChildIndex + 1, 0, movedPhase)
        setLineItems(newItems)
      } else {
        // Dropping on another phase → reparent to same parent, insert before target
        movedPhase.parentProjectUid = targetItem.parentProjectUid
        const targetIndex = remaining.findIndex((li) => li.uid === targetUid)
        const newItems = [...remaining]
        newItems.splice(targetIndex, 0, movedPhase)
        setLineItems(newItems)
      }
    }

    setRowDragUid(null)
    setRowDropTargetUid(null)
  }

  const handleRowDragEnd = () => {
    setRowDragUid(null)
    setRowDropTargetUid(null)
  }

  const handleSave = () => {
    persistToLocalStorage()

    const payload: PersistedState = {
      nextId,
      lineItems,
      collapsedProjectIds: Array.from(collapsedProjects),
      disciplines,
      teamCapacities,
      schedulerOverrides,
    }

    const blob = new Blob([JSON.stringify(payload, null, 2)], {
      type: 'application/json;charset=utf-8',
    })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = `project-data-${new Date().toISOString().slice(0, 10)}.json`
    link.click()
    URL.revokeObjectURL(url)
    setStatusMessage('Saved file downloaded.')
  }

  const handleCopyShareLink = async () => {
    const payload: PersistedState = {
      nextId,
      lineItems,
      collapsedProjectIds: Array.from(collapsedProjects),
      disciplines,
      teamCapacities,
      schedulerOverrides,
    }
    try {
      const json = JSON.stringify(payload)
      const encoded = await compressToBase64(json)
      const shareUrl = `${window.location.origin}${window.location.pathname}#data=${encoded}`
      await navigator.clipboard.writeText(shareUrl)
      setStatusMessage(`Share link copied to clipboard (${(shareUrl.length / 1024).toFixed(1)} KB)`)
    } catch {
      setStatusMessage('Failed to generate share link.')
    }
  }

  const handleLoad = () => {
    loadFileInputRef.current?.click()
  }

  const handleLoadFile = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) {
      return
    }

    const reader = new FileReader()
    reader.onload = () => {
      try {
        rememberForUndo()
        const content = String(reader.result ?? '')
        const parsed = JSON.parse(content) as PersistedState
        if (!Array.isArray(parsed.lineItems)) {
          throw new Error('Invalid file format.')
        }
        loadFromPayload(parsed)
        setStatusMessage('Loaded data from JSON file.')
      } catch {
        setStatusMessage('Unable to load file. Please choose a valid saved JSON file.')
      }
    }
    reader.readAsText(file)
    event.target.value = ''
  }

  const handleUndo = () => {
    if (undoStack.length === 0) {
      setStatusMessage('Nothing to undo.')
      return
    }

    const previous = undoStack[undoStack.length - 1]
    setUndoStack((stack) => stack.slice(0, -1))
    loadFromPayload(previous)
    setStatusMessage('Undo applied.')
  }

  const handleClearData = () => {
    const confirmed = window.confirm(
      'Clear all project data? This will remove current rows and saved browser data.',
    )

    if (!confirmed) {
      return
    }

    rememberForUndo()
    setLineItems([])
    setNextId(1)
    setCollapsedProjects(new Set())
    setSchedulerOverrides({})
    setSchedulerUndoStack([])
    setSchedulerRedoStack([])
    setDependencyDrafts({})
    localStorage.removeItem(STORAGE_KEY)
    setStatusMessage('All data cleared.')
  }

  const schedulerData = useMemo(() => {
    const projects = lineItems.filter((item) => isProjectRow(item))

    const tasks: Task[] = []
    projects.forEach((project, projectOrder) => {
      const phases = lineItems.filter((item) => item.parentProjectUid === project.uid)
      const priority = numberOrZero(project.valueScore)

      if (phases.length > 0) {
        phases.forEach((phase, phaseOrder) => {
          tasks.push({
            id: phase.uid,
            name: phase.name || `Phase ${phase.uid}`,
            projectId: project.uid,
            projectName: project.name || `Project ${project.uid}`,
            priority,
            duration: Math.max(1, numberOrZero(phase.durationMonths)),
            dependencyIds: [...phase.dependencyUids],
            requirements: disciplines.reduce<Record<string, number>>((acc, team) => {
              acc[team] = numberOrZero(phase.resourceAllocation[team])
              return acc
            }, {} as Record<string, number>),
            order: projectOrder * 1000 + phaseOrder,
            isStandaloneProject: false,
          })
        })
      } else {
        tasks.push({
          id: project.uid,
          name: project.name || `Project ${project.uid}`,
          projectId: project.uid,
          projectName: project.name || `Project ${project.uid}`,
          priority,
          duration: Math.max(1, numberOrZero(project.durationMonths)),
          dependencyIds: [...project.dependencyUids],
          requirements: disciplines.reduce<Record<string, number>>((acc, team) => {
            acc[team] = numberOrZero(project.resourceAllocation[team])
            return acc
          }, {} as Record<string, number>),
          order: projectOrder * 1000,
          isStandaloneProject: true,
        })
      }
    })

    // Sort all tasks by priority then order (same algorithm always)
    const sortedTasks = [...tasks].sort((left, right) => {
      if (right.priority !== left.priority) {
        return right.priority - left.priority
      }
      return left.order - right.order
    })

    // --- Step 1: Compute base schedule (capacity-based, ignoring all overrides) ---
    const baseUsageByMonth: Record<string, number[]> = disciplines.reduce((acc, team) => {
      acc[team] = []
      return acc
    }, {} as Record<string, number[]>)

    const baseScheduledById = new Map<number, { startMonth: number; endMonth: number }>()

    const baseCanFitAtMonth = (task: Task, startMonth: number): boolean => {
      for (let offset = 0; offset < task.duration; offset += 1) {
        const monthIndex = startMonth + offset
        for (const team of disciplines) {
          const required = task.requirements[team]
          if (required <= 0) continue
          const capacity = numberOrZero(teamCapacities[team])
          if (capacity <= 0) return false
          const used = baseUsageByMonth[team][monthIndex] ?? 0
          if (used + required > capacity + 1e-9) return false
        }
      }
      return true
    }

    const baseAllocate = (task: Task, startMonth: number) => {
      for (let offset = 0; offset < task.duration; offset += 1) {
        const monthIndex = startMonth + offset
        for (const team of disciplines) {
          baseUsageByMonth[team][monthIndex] = (baseUsageByMonth[team][monthIndex] ?? 0) + task.requirements[team]
        }
      }
      baseScheduledById.set(task.id, { startMonth, endMonth: startMonth + task.duration })
    }

    sortedTasks.forEach((task) => {
      const dependencyEnd = task.dependencyIds.reduce((maxEnd, dependencyId) => {
        const dep = baseScheduledById.get(dependencyId)
        return dep ? Math.max(maxEnd, dep.endMonth) : maxEnd
      }, 0)

      let startMonth = dependencyEnd
      let searchCount = 0
      while (!baseCanFitAtMonth(task, startMonth) && searchCount < 240) {
        startMonth += 1
        searchCount += 1
      }
      baseAllocate(task, startMonth)
    })

    // --- Step 2: Apply overrides on top of base schedule ---
    // Frozen tasks use their override position.
    // Non-frozen overrides (from rebalance) also use their override position.
    // Tasks with no override keep their base-scheduled positions.
    const scheduledTasks: ScheduledTask[] = sortedTasks.map((task) => {
      const override = schedulerOverrides[task.id]
      if (override) {
        const overrideStart = Math.max(0, Math.floor(override.startMonth))
        return {
          ...task,
          startMonth: overrideStart,
          endMonth: overrideStart + task.duration,
          frozen: Boolean(override.frozen),
        }
      }
      const base = baseScheduledById.get(task.id)!
      return {
        ...task,
        startMonth: base.startMonth,
        endMonth: base.endMonth,
        frozen: false,
      }
    })

    // --- Step 3: Compute utilization from final positions ---
    const finalUsageByMonth: Record<string, number[]> = disciplines.reduce((acc, team) => {
      acc[team] = []
      return acc
    }, {} as Record<string, number[]>)

    scheduledTasks.forEach((task) => {
      for (let offset = 0; offset < task.duration; offset += 1) {
        const monthIndex = task.startMonth + offset
        for (const team of disciplines) {
          finalUsageByMonth[team][monthIndex] = (finalUsageByMonth[team][monthIndex] ?? 0) + task.requirements[team]
        }
      }
    })

    const maxEndMonth = Math.max(24, ...scheduledTasks.map((task) => task.endMonth + 6))

    const utilizationByTeam: Record<string, number[]> = disciplines.reduce((acc, team) => {
      const capacity = numberOrZero(teamCapacities[team])
      acc[team] = Array.from({ length: maxEndMonth }, (_, monthIndex) => {
        const used = finalUsageByMonth[team][monthIndex] ?? 0
        if (capacity <= 0) {
          return used > 0 ? 100 : 0
        }
        return (used / capacity) * 100
      })
      return acc
    }, {} as Record<string, number[]>)

    return {
      tasks: scheduledTasks,
      maxEndMonth,
      utilizationByTeam,
    }
  }, [schedulerOverrides, lineItems, teamCapacities, disciplines])

  // Display tasks with drag preview overrides applied (for visual feedback without recalculation)
  const displayTasks = useMemo(() => {
    if (!dragPreviewOverrides) {
      return schedulerData.tasks
    }
    
    return schedulerData.tasks.map((task) => {
      const override = dragPreviewOverrides[task.id]
      if (override) {
        return {
          ...task,
          startMonth: override.startMonth,
          endMonth: override.startMonth + task.duration,
          frozen: override.frozen,
        }
      }
      return task
    })
  }, [schedulerData.tasks, dragPreviewOverrides])

  // Calculate real-time utilization based on displayTasks (updates during drag)
  const displayUtilization = useMemo(() => {
    const maxEndMonth = Math.max(24, ...displayTasks.map((task) => task.endMonth + 6))
    const usageByMonth: Record<string, number[]> = disciplines.reduce((acc, team) => {
      acc[team] = []
      return acc
    }, {} as Record<string, number[]>)

    displayTasks.forEach((task) => {
      disciplines.forEach((team) => {
        const requirement = task.requirements[team] ?? 0
        if (requirement > 0) {
          for (let month = task.startMonth; month < task.endMonth; month++) {
            usageByMonth[team][month] = (usageByMonth[team][month] ?? 0) + requirement
          }
        }
      })
    })

    const utilizationByTeam: Record<string, number[]> = disciplines.reduce((acc, team) => {
      const capacity = numberOrZero(teamCapacities[team])
      acc[team] = Array.from({ length: maxEndMonth }, (_, monthIndex) => {
        const used = usageByMonth[team][monthIndex] ?? 0
        if (capacity <= 0) {
          return used > 0 ? 100 : 0
        }
        return (used / capacity) * 100
      })
      return acc
    }, {} as Record<string, number[]>)

    return { maxEndMonth, utilizationByTeam }
  }, [displayTasks, teamCapacities, disciplines])

  const buildDependencyConstrainedOverrides = (
    taskId: number,
    proposedStartMonth: number,
    baseTasks: ScheduledTask[],
    baseOverrides: Record<number, SchedulerOverride>,
  ): Record<number, SchedulerOverride> => {
    const byId = new Map(baseTasks.map((task) => [task.id, task]))
    const task = byId.get(taskId)
    if (!task) {
      return cloneOverrides(baseOverrides)
    }
    
    // Check if dragged task is a parent project with phases
    const draggedItem = lineItems.find((item) => item.uid === taskId)
    const childPhases = lineItems.filter((item) => item.parentProjectUid === taskId)
    const isParentProject = draggedItem && !draggedItem.parentProjectUid && childPhases.length > 0

    const originalStarts = new Map(baseTasks.map((currentTask) => [currentTask.id, currentTask.startMonth]))

    const evaluateCandidate = (candidateStart: number) => {
      const simulatedStarts = new Map(originalStarts)
      simulatedStarts.set(taskId, candidateStart)

      let changed = true
      let guard = 0
      while (changed && guard < baseTasks.length * baseTasks.length) {
        changed = false
        guard += 1

        baseTasks.forEach((currentTask) => {
          const requiredStart = currentTask.dependencyIds.reduce((latestDependencyEnd, dependencyId) => {
            const dependencyTask = byId.get(dependencyId)
            if (!dependencyTask) {
              return latestDependencyEnd
            }
            const dependencyStart = simulatedStarts.get(dependencyId) ?? dependencyTask.startMonth
            const dependencyEnd = dependencyStart + dependencyTask.duration
            return Math.max(latestDependencyEnd, dependencyEnd)
          }, 0)

          const currentStart = simulatedStarts.get(currentTask.id) ?? currentTask.startMonth
          if (requiredStart > currentStart) {
            const isFrozen = baseOverrides[currentTask.id]?.frozen === true
            if (isFrozen && currentTask.id !== taskId) {
              changed = false
              guard = baseTasks.length * baseTasks.length
              simulatedStarts.set(-1, -1)
              return
            }
            simulatedStarts.set(currentTask.id, requiredStart)
            changed = true
          }
        })
      }

      if (simulatedStarts.get(-1) === -1) {
        return { ok: false as const }
      }

      return { ok: true as const, starts: simulatedStarts }
    }

    const minAllowedFromDependencies = task.dependencyIds.reduce((latestDependencyEnd, dependencyId) => {
      const dependencyTask = byId.get(dependencyId)
      if (!dependencyTask) {
        return latestDependencyEnd
      }
      const dependencyStart = originalStarts.get(dependencyId) ?? dependencyTask.startMonth
      return Math.max(latestDependencyEnd, dependencyStart + dependencyTask.duration)
    }, 0)

    const clampedProposed = Math.max(0, Math.floor(proposedStartMonth), minAllowedFromDependencies)
    const originalStart = originalStarts.get(taskId) ?? 0
    const moveDelta = clampedProposed - originalStart

    const tryBuildOverrides = (candidateStart: number) => {
      const evaluation = evaluateCandidate(candidateStart)
      if (!evaluation.ok) {
        return null
      }

      const nextOverrides = cloneOverrides(baseOverrides)
      nextOverrides[taskId] = {
        startMonth: candidateStart,
        frozen: true,
      }
      
      // If dragging a parent project, also move all child phases by the same delta
      if (isParentProject) {
        childPhases.forEach((phase) => {
          const phaseTask = byId.get(phase.uid)
          if (phaseTask) {
            const phaseOriginalStart = originalStarts.get(phase.uid) ?? 0
            const phaseNewStart = Math.max(0, phaseOriginalStart + moveDelta)
            nextOverrides[phase.uid] = {
              startMonth: phaseNewStart,
              frozen: true,
            }
          }
        })
      }

      evaluation.starts.forEach((simulatedStart, currentTaskId) => {
        if (currentTaskId < 0 || currentTaskId === taskId) {
          return
        }
        // Skip child phases if we're dragging their parent (already handled above)
        if (isParentProject && childPhases.some((p) => p.uid === currentTaskId)) {
          return
        }
        const isFrozen = baseOverrides[currentTaskId]?.frozen === true
        if (isFrozen) {
          return
        }

        const originalStartMonth = originalStarts.get(currentTaskId)
        if (originalStartMonth !== undefined && simulatedStart > originalStartMonth) {
          nextOverrides[currentTaskId] = {
            startMonth: simulatedStart,
            frozen: false,
          }
        }
      })

      return nextOverrides
    }

    if (clampedProposed <= originalStart) {
      const direct = tryBuildOverrides(clampedProposed)
      if (direct) {
        return direct
      }
      return tryBuildOverrides(originalStart) ?? cloneOverrides(baseOverrides)
    }

    let candidate = clampedProposed
    while (candidate >= originalStart) {
      const evaluated = tryBuildOverrides(candidate)
      if (evaluated) {
        return evaluated
      }
      candidate -= 1
    }

    return tryBuildOverrides(originalStart) ?? cloneOverrides(baseOverrides)
  }

  const handleSchedulerUndo = () => {
    if (schedulerUndoStack.length === 0) {
      setStatusMessage('No scheduler changes to undo.')
      return
    }

    const previous = schedulerUndoStack[schedulerUndoStack.length - 1]
    setSchedulerUndoStack((stack) => stack.slice(0, -1))
    setSchedulerRedoStack((stack) => [...stack, cloneOverrides(schedulerOverrides)])
    setSchedulerOverrides(cloneOverrides(previous))
    setStatusMessage('Scheduler undo applied.')
  }

  const handleSchedulerRedo = () => {
    if (schedulerRedoStack.length === 0) {
      setStatusMessage('No scheduler changes to redo.')
      return
    }

    const next = schedulerRedoStack[schedulerRedoStack.length - 1]
    setSchedulerRedoStack((stack) => stack.slice(0, -1))
    setSchedulerUndoStack((stack) => [...stack, cloneOverrides(schedulerOverrides)])
    setSchedulerOverrides(cloneOverrides(next))
    setStatusMessage('Scheduler redo applied.')
  }

  const handleRecalculateAroundFrozen = () => {
    // Build task list (same as schedulerData)
    const projects = lineItems.filter((item) => isProjectRow(item))
    const tasks: Task[] = []
    projects.forEach((project, projectOrder) => {
      const phases = lineItems.filter((item) => item.parentProjectUid === project.uid)
      const priority = numberOrZero(project.valueScore)
      if (phases.length > 0) {
        phases.forEach((phase, phaseOrder) => {
          tasks.push({
            id: phase.uid,
            name: phase.name || `Phase ${phase.uid}`,
            projectId: project.uid,
            projectName: project.name || `Project ${project.uid}`,
            priority,
            duration: Math.max(1, numberOrZero(phase.durationMonths)),
            dependencyIds: [...phase.dependencyUids],
            requirements: disciplines.reduce<Record<string, number>>((acc, team) => {
              acc[team] = numberOrZero(phase.resourceAllocation[team])
              return acc
            }, {} as Record<string, number>),
            order: projectOrder * 1000 + phaseOrder,
            isStandaloneProject: false,
          })
        })
      } else {
        tasks.push({
          id: project.uid,
          name: project.name || `Project ${project.uid}`,
          projectId: project.uid,
          projectName: project.name || `Project ${project.uid}`,
          priority,
          duration: Math.max(1, numberOrZero(project.durationMonths)),
          dependencyIds: [...project.dependencyUids],
          requirements: disciplines.reduce<Record<string, number>>((acc, team) => {
            acc[team] = numberOrZero(project.resourceAllocation[team])
            return acc
          }, {} as Record<string, number>),
          order: projectOrder * 1000,
          isStandaloneProject: true,
        })
      }
    })

    // Separate frozen and non-frozen
    const frozenTasks: Task[] = []
    const nonFrozenTasks: Task[] = []
    tasks.forEach((task) => {
      if (schedulerOverrides[task.id]?.frozen) {
        frozenTasks.push(task)
      } else {
        nonFrozenTasks.push(task)
      }
    })

    // Sort non-frozen by priority desc, then order asc
    nonFrozenTasks.sort((a, b) => {
      if (b.priority !== a.priority) return b.priority - a.priority
      return a.order - b.order
    })

    // Usage tracker
    const usageByMonth: Record<string, number[]> = disciplines.reduce((acc, team) => {
      acc[team] = []
      return acc
    }, {} as Record<string, number[]>)

    const allocate = (task: Task, startMonth: number) => {
      for (let offset = 0; offset < task.duration; offset += 1) {
        const monthIndex = startMonth + offset
        for (const team of disciplines) {
          usageByMonth[team][monthIndex] = (usageByMonth[team][monthIndex] ?? 0) + task.requirements[team]
        }
      }
    }

    const canFitAtMonth = (task: Task, startMonth: number): boolean => {
      for (let offset = 0; offset < task.duration; offset += 1) {
        const monthIndex = startMonth + offset
        for (const team of disciplines) {
          const required = task.requirements[team]
          if (required <= 0) continue
          const capacity = numberOrZero(teamCapacities[team])
          if (capacity <= 0) return false
          const used = usageByMonth[team][monthIndex] ?? 0
          if (used + required > capacity + 1e-9) return false
        }
      }
      return true
    }

    const scheduledById = new Map<number, number>() // taskId -> startMonth
    const newOverrides: Record<number, SchedulerOverride> = {}

    // Step 1: Place all frozen tasks first (they consume capacity but don't move)
    frozenTasks.forEach((task) => {
      const frozenStart = Math.max(0, Math.floor(schedulerOverrides[task.id].startMonth))
      allocate(task, frozenStart)
      scheduledById.set(task.id, frozenStart)
      newOverrides[task.id] = { startMonth: frozenStart, frozen: true }
    })

    // Step 2: Schedule non-frozen tasks around frozen ones, respecting capacity + dependencies
    nonFrozenTasks.forEach((task) => {
      const dependencyEnd = task.dependencyIds.reduce((maxEnd, depId) => {
        const depStart = scheduledById.get(depId)
        if (depStart === undefined) return maxEnd
        const depTask = tasks.find((t) => t.id === depId)
        const depEnd = depStart + (depTask?.duration ?? 0)
        return Math.max(maxEnd, depEnd)
      }, 0)

      let startMonth = dependencyEnd
      let searchCount = 0
      while (!canFitAtMonth(task, startMonth) && searchCount < 240) {
        startMonth += 1
        searchCount += 1
      }
      allocate(task, startMonth)
      scheduledById.set(task.id, startMonth)
      // Non-frozen tasks get overrides too so they stay in their new positions
      newOverrides[task.id] = { startMonth, frozen: false }
    })

    pushSchedulerHistory(schedulerOverrides)
    setSchedulerOverrides(newOverrides)
    setStatusMessage('Rebalanced: frozen tasks kept in place, others scheduled around them by priority.')
  }

  const handleResetScheduler = () => {
    setSchedulerOverrides({})
    setStatusMessage('Scheduler reset to auto-calculated schedule.')
  }

  useEffect(() => {
    if (dragTaskId === null) {
      return
    }

    if (!dragBaseTasks || !dragBaseOverrides) {
      return
    }

    const handleMove = (event: MouseEvent) => {
      const deltaPixels = event.clientX - dragStartX
      const deltaMonths = Math.round(deltaPixels / pixelsPerMonth)
      const proposedMonth = Math.max(0, dragInitialMonth + deltaMonths)

      // Check if dragging a parent project with phases
      const draggedItem = lineItems.find((item) => item.uid === dragTaskId)
      const childPhases = lineItems.filter((item) => item.parentProjectUid === dragTaskId)
      const isParentProject = draggedItem && !draggedItem.parentProjectUid && childPhases.length > 0

      // Start with base overrides
      const nextOverrides = cloneOverrides(dragBaseOverrides)

      // Helper to get task start considering overrides applied so far
      const getTaskStart = (taskId: number): number => {
        if (nextOverrides[taskId]) return nextOverrides[taskId].startMonth
        const task = dragBaseTasks.find((t) => t.id === taskId)
        return task?.startMonth ?? 0
      }

      const getTaskEnd = (taskId: number): number => {
        const task = dragBaseTasks.find((t) => t.id === taskId)
        if (!task) return 0
        return getTaskStart(taskId) + task.duration
      }

      // Calculate the earliest possible start for a task by walking its full predecessor chain
      // back to month 0. This includes cross-project dependencies.
      const getMinStartFromPredecessorChain = (taskId: number, visited: Set<number> = new Set()): number => {
        if (visited.has(taskId)) return 0
        visited.add(taskId)
        const task = dragBaseTasks.find((t) => t.id === taskId)
        if (!task) return 0
        let totalPredDuration = 0
        task.dependencyIds.forEach((predId) => {
          const pred = dragBaseTasks.find((t) => t.id === predId)
          if (!pred) return
          const predChainMin = pred.duration + getMinStartFromPredecessorChain(predId, visited)
          totalPredDuration = Math.max(totalPredDuration, predChainMin)
        })
        return totalPredDuration
      }

      // Push dependents forward (right) if dragged item's end overlaps their start.
      // Works across projects to enforce cross-project dependencies.
      const pushDependentsForward = (taskId: number, visited: Set<number> = new Set()) => {
        if (visited.has(taskId)) return
        visited.add(taskId)
        const taskEnd = getTaskEnd(taskId)
        dragBaseTasks.forEach((dep) => {
          if (!dep.dependencyIds.includes(taskId)) return
          const currentStart = getTaskStart(dep.id)
          if (currentStart < taskEnd) {
            nextOverrides[dep.id] = { startMonth: taskEnd, frozen: true }
            pushDependentsForward(dep.id, visited)
          }
        })
      }

      // Push predecessors backward (left) if dragged item's start overlaps their end.
      // Works across projects to enforce cross-project dependencies.
      const pushPredecessorsBackward = (taskId: number, visited: Set<number> = new Set()) => {
        if (visited.has(taskId)) return
        visited.add(taskId)
        const taskStart = getTaskStart(taskId)
        const task = dragBaseTasks.find((t) => t.id === taskId)
        if (!task) return
        task.dependencyIds.forEach((predId) => {
          const pred = dragBaseTasks.find((t) => t.id === predId)
          if (!pred) return
          const predEnd = getTaskEnd(predId)
          if (predEnd > taskStart) {
            const newStart = Math.max(0, taskStart - pred.duration)
            nextOverrides[predId] = { startMonth: newStart, frozen: true }
            pushPredecessorsBackward(predId, visited)
          }
        })
      }

      if (isParentProject && childPhases.length > 0) {
        // Dragging a parent project: move all phases together
        // First, find the most constrained phase to determine the minimum allowed delta
        let minAllowedDelta = proposedMonth - dragInitialMonth
        childPhases.forEach((phase) => {
          const phaseOriginalTask = dragBaseTasks.find((t) => t.id === phase.uid)
          if (phaseOriginalTask) {
            const minStart = getMinStartFromPredecessorChain(phase.uid)
            const phaseProposed = phaseOriginalTask.startMonth + minAllowedDelta
            if (phaseProposed < minStart) {
              // This phase would push predecessors past month 0; constrain the delta
              minAllowedDelta = minStart - phaseOriginalTask.startMonth
            }
          }
        })
        
        childPhases.forEach((phase) => {
          const phaseOriginalTask = dragBaseTasks.find((t) => t.id === phase.uid)
          if (phaseOriginalTask) {
            const newStart = phaseOriginalTask.startMonth + minAllowedDelta
            nextOverrides[phase.uid] = { startMonth: newStart, frozen: true }
          }
        })
        // Push in both directions across all projects
        childPhases.forEach((phase) => {
          pushDependentsForward(phase.uid)
          pushPredecessorsBackward(phase.uid)
        })
      } else {
        // Dragging a standalone project or individual phase
        // Clamp proposed month so predecessor chain doesn't go below month 0
        const minStart = getMinStartFromPredecessorChain(dragTaskId)
        const clampedMonth = Math.max(proposedMonth, minStart)
        
        nextOverrides[dragTaskId] = { startMonth: clampedMonth, frozen: true }
        pushDependentsForward(dragTaskId)
        pushPredecessorsBackward(dragTaskId)
      }
      
      setDragPreviewOverrides(nextOverrides)
    }

    const handleUp = () => {
      if (!dragPreviewOverrides) {
        setDragTaskId(null)
        setDragBaseTasks(null)
        setDragBaseOverrides(null)
        return
      }

      pushSchedulerHistory(schedulerOverrides)
      setSchedulerOverrides(cloneOverrides(dragPreviewOverrides))
      setDragPreviewOverrides(null)
      setDragTaskId(null)
      setDragBaseTasks(null)
      setDragBaseOverrides(null)
      setStatusMessage('Task manually moved and frozen.')
    }

    window.addEventListener('mousemove', handleMove)
    window.addEventListener('mouseup', handleUp)
    return () => {
      window.removeEventListener('mousemove', handleMove)
      window.removeEventListener('mouseup', handleUp)
    }
  }, [
    buildDependencyConstrainedOverrides,
    dragBaseOverrides,
    dragBaseTasks,
    dragInitialMonth,
    dragPreviewOverrides,
    dragStartX,
    dragTaskId,
    pixelsPerMonth,
    schedulerOverrides,
  ])

  useEffect(() => {
    persistToLocalStorage()
  }, [collapsedProjects, lineItems, nextId, schedulerOverrides, teamCapacities, disciplines])

  // Measure thead row heights and set sticky top offsets directly on <th> elements
  useLayoutEffect(() => {
    const table = tableRef.current
    if (!table) return
    const thead = table.querySelector('thead')
    if (!thead) return
    const rows = Array.from(thead.querySelectorAll('tr'))
    if (rows.length < 2) return

    const applyOffsets = () => {
      const h0 = rows[0].getBoundingClientRect().height
      const h1 = rows[1] ? rows[1].getBoundingClientRect().height : 0

      // Row 1 cells: top = 0 (already set via CSS default)
      // Row 2 cells: top = height of row 1
      if (rows[1]) {
        const cells = rows[1].querySelectorAll('th')
        cells.forEach((th) => {
          ;(th as HTMLElement).style.top = `${h0}px`
        })
      }
      // Row 3 cells: top = height of row 1 + row 2
      if (rows[2]) {
        const cells = rows[2].querySelectorAll('th')
        cells.forEach((th) => {
          ;(th as HTMLElement).style.top = `${h0 + h1}px`
        })
      }
    }

    applyOffsets()

    const observer = new ResizeObserver(applyOffsets)
    rows.forEach((r) => observer.observe(r))
    return () => observer.disconnect()
  })

  useEffect(() => {
    if (pendingFocusRowId === null) {
      return
    }

    const targetInput = nameInputRefs.current[pendingFocusRowId]
    if (targetInput) {
      targetInput.focus()
      targetInput.select()
      setPendingFocusRowId(null)
    }
  }, [lineItems, pendingFocusRowId])

  const visibleRows = useMemo(
    () =>
      lineItems.filter((item) => {
        if (isProjectRow(item)) {
          return true
        }
        return !collapsedProjects.has(item.parentProjectUid ?? -1)
      }),
    [collapsedProjects, lineItems],
  )

  const projectDurationByUid = useMemo(() => {
    const durationMap = new Map<number, number>()
    lineItems
      .filter((item) => isProjectRow(item))
      .forEach((project) => {
        const duration = calculateProjectDuration(lineItems, project.uid)
        durationMap.set(project.uid, duration)
      })
    return durationMap
  }, [lineItems])

  const SCHEDULER_LABEL_WIDTH_KEY = 'portfolio-scheduler-label-width'
  const [schedulerLabelWidth, setSchedulerLabelWidth] = useState(() => {
    try {
      const saved = localStorage.getItem(SCHEDULER_LABEL_WIDTH_KEY)
      if (saved) return Math.max(100, Number(saved))
    } catch { /* ignore */ }
    return 300
  })

  const handleSchedulerLabelResizeStart = (event: React.MouseEvent) => {
    event.preventDefault()
    event.stopPropagation()
    const startX = event.clientX
    const startW = schedulerLabelWidth
    const onMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientX - startX
      const newWidth = Math.max(100, startW + delta)
      setSchedulerLabelWidth(newWidth)
      localStorage.setItem(SCHEDULER_LABEL_WIDTH_KEY, String(newWidth))
    }
    const onUp = () => {
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
    }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }
  // Calculate minimum months to fill full viewport width (handle large screens and zoomed out views)
  // Use 2400px to ensure timeline always extends beyond typical viewport widths
  const minMonthsForFullScreen = Math.ceil(2400 / pixelsPerMonth)
  const displayMonths = Math.max(displayUtilization.maxEndMonth, minMonthsForFullScreen)
  const timelineWidth = displayMonths * pixelsPerMonth
  const schedulerCanvasWidth = schedulerLabelWidth + timelineWidth

  // Build two-row calendar header: year row + month/quarter row (adaptive based on zoom)
  const showQuarters = timeGranularity === 'quarterly' || (timeGranularity === 'auto' && pixelsPerMonth < 46)
  const calendarTimeUnits = showQuarters ? Math.ceil(displayMonths / 3) : displayMonths
  const calendarUnitWidth = showQuarters ? pixelsPerMonth * 3 : pixelsPerMonth

  const yearSpans: Array<{ year: number; startIndex: number; count: number }> = []
  const timeLabels: string[] = []

  for (let i = 0; i < calendarTimeUnits; i++) {
    const monthIndex = showQuarters ? i * 3 : i
    const labelDate = new Date(timelineStart)
    labelDate.setMonth(labelDate.getMonth() + monthIndex)
    const year = labelDate.getFullYear()
    const lastYearSpan = yearSpans[yearSpans.length - 1]

    if (lastYearSpan && lastYearSpan.year === year) {
      lastYearSpan.count++
    } else {
      yearSpans.push({ year, startIndex: i, count: 1 })
    }

    timeLabels.push(
      showQuarters
        ? `Q${Math.floor(labelDate.getMonth() / 3) + 1}`
        : labelDate.toLocaleDateString(undefined, { month: 'short' }),
    )
  }

  // Group tasks by project
  interface TaskGroup {
    projectId: number
    projectName: string
    projectTask: ScheduledTask | null
    phaseTasks: ScheduledTask[]
  }

  const taskGroups: TaskGroup[] = []
  const projectMap = new Map<number, TaskGroup>()

  displayTasks.forEach((task) => {
    const lineItem = lineItems.find((item) => item.uid === task.id)
    const isPhase = lineItem?.parentProjectUid !== null

    if (isPhase && lineItem) {
      const projectId = lineItem.parentProjectUid!
      let group = projectMap.get(projectId)
      if (!group) {
        const projectItem = lineItems.find((p) => p.uid === projectId)
        group = {
          projectId,
          projectName: projectItem?.name ?? 'Unknown Project',
          projectTask: null,
          phaseTasks: [],
        }
        projectMap.set(projectId, group)
        taskGroups.push(group)
      }
      group.phaseTasks.push(task)
    } else {
      let group = projectMap.get(task.id)
      if (!group) {
        group = {
          projectId: task.id,
          projectName: task.name,
          projectTask: task,
          phaseTasks: [],
        }
        projectMap.set(task.id, group)
        taskGroups.push(group)
      } else {
        group.projectTask = task
      }
    }
  })

  // Sort taskGroups by value score descending (highest priority first)
  taskGroups.sort((a, b) => {
    const aProject = lineItems.find((item) => item.uid === a.projectId)
    const bProject = lineItems.find((item) => item.uid === b.projectId)
    const aScore = numberOrZero(aProject?.valueScore ?? null)
    const bScore = numberOrZero(bProject?.valueScore ?? null)
    if (bScore !== aScore) return bScore - aScore
    // Tie-break by original lineItems order
    const aIndex = lineItems.findIndex((item) => item.uid === a.projectId)
    const bIndex = lineItems.findIndex((item) => item.uid === b.projectId)
    return aIndex - bIndex
  })

  // Sort phaseTasks within each group to match original lineItems order
  taskGroups.forEach((group) => {
    group.phaseTasks.sort((a, b) => {
      const aIndex = lineItems.findIndex((item) => item.uid === a.id)
      const bIndex = lineItems.findIndex((item) => item.uid === b.id)
      return aIndex - bIndex
    })
  })

  // For projects with only phases (no direct project task), synthesize a project bar from phase spans
  taskGroups.forEach((group) => {
    if (!group.projectTask && group.phaseTasks.length > 0) {
      const minStart = Math.min(...group.phaseTasks.map((p) => p.startMonth))
      const maxEnd = Math.max(...group.phaseTasks.map((p) => p.startMonth + p.duration))
      const anyPhaseFrozen = group.phaseTasks.some((p) => p.frozen)
      const projectItem = lineItems.find((item) => item.uid === group.projectId)
      
      // Aggregate dependencies and requirements from all phases
      const allDependencyIds = new Set<number>()
      const aggregateRequirements: Record<string, number> = { ...emptyAllocation(disciplines) } as Record<string, number>
      
      group.phaseTasks.forEach((phase) => {
        phase.dependencyIds.forEach((depId) => allDependencyIds.add(depId))
        disciplines.forEach((key) => {
          aggregateRequirements[key] = (aggregateRequirements[key] || 0) + (phase.requirements[key] || 0)
        })
      })
      
      group.projectTask = {
        id: group.projectId,
        projectId: group.projectId,
        name: group.projectName,
        projectName: group.projectName,
        startMonth: minStart,
        duration: maxEnd - minStart,
        endMonth: maxEnd,
        priority: numberOrZero(projectItem?.valueScore ?? null),
        frozen: anyPhaseFrozen,
        dependencyIds: Array.from(allDependencyIds),
        requirements: aggregateRequirements,
        order: Math.min(...group.phaseTasks.map((p) => p.order)),
        isStandaloneProject: false,
      }
    }
  })

  const now = new Date()
  const todayMonthFloat =
    (now.getFullYear() - timelineStart.getFullYear()) * 12 +
    (now.getMonth() - timelineStart.getMonth()) +
    (now.getDate() - 1) / 30

  const histogramBucketSize = showQuarters ? 3 : 1
  const histogramBuckets = Array.from(
    { length: Math.ceil(displayMonths / histogramBucketSize) },
    (_, bucketIndex) => {
      const bucketStart = bucketIndex * histogramBucketSize
      const bucketEnd = Math.min(displayMonths, bucketStart + histogramBucketSize)
      return { bucketStart, bucketEnd }
    },
  )

  return (
    <div className="app-shell">
      <nav className="app-tab-bar" aria-label="Application tabs">
        <span className="app-title">Portfolio Optimization Tool</span>
        <button
          className={activeTab === 'project-data' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('project-data')}
          type="button"
        >
          Project Data
        </button>
        <button
          className={activeTab === 'scheduler' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('scheduler')}
          type="button"
        >
          Scheduler
        </button>
      </nav>

      {activeTab === 'project-data' ? (
        <section className="tab-content">
          <div className="toolbar">
            <button onClick={addProject} type="button">
              + Add Project
            </button>
            <button onClick={handleSave} type="button">
              Save File
            </button>
            <button onClick={handleLoad} type="button">
              Load File
            </button>
            <button onClick={handleCopyShareLink} type="button">
              Copy Share Link
            </button>
            <button onClick={handleUndo} type="button" disabled={undoStack.length === 0}>
              Undo
            </button>
            <button onClick={handleClearData} type="button" className="danger-button">
              Clear Data
            </button>
            <span className="toolbar-separator" />
            <button
              type="button"
              className="icon-btn"
              title="Expand all projects"
              onClick={() => setCollapsedProjects(new Set())}
            >
              ⊞
            </button>
            <button
              type="button"
              className="icon-btn"
              title="Collapse all projects"
              onClick={() => {
                const allParents = lineItems
                  .filter((item) => isProjectRow(item) && lineItems.some((c) => c.parentProjectUid === item.uid))
                  .map((item) => item.uid)
                setCollapsedProjects(new Set(allParents))
              }}
            >
              ⊟
            </button>
            <span className="hint">Use Tab/Shift+Tab to indent/outdent and Enter to add rows.</span>
            {statusMessage ? <span className="status-text">{statusMessage}</span> : null}
          </div>

          <div className="table-wrap">
            <table ref={tableRef} onKeyDown={handleTableArrowNav} style={{ tableLayout: 'fixed', width: COL_KEYS.reduce((sum, col) => sum + (effectiveColWidths[col] ?? 58), 0) }}>
              <colgroup>
                {COL_KEYS.map((col) => (
                  <col key={col} style={{ width: effectiveColWidths[col] ?? 58 }} />
                ))}
              </colgroup>
              <thead>
                <tr>
                  <th colSpan={6} className="capacity-label">
                    Team Capacity (FTE)
                  </th>
                  {disciplines.map((team) => (
                    <th key={`${team}-cap`} className="numeric-cell">
                      <input
                        aria-label={`${team} capacity`}
                        type="number"
                        min={0}
                        step="0.1"
                        value={toNumberInputValue(teamCapacities[team])}
                        placeholder="Cap"
                        onChange={(event) =>
                          setTeamCapacities((previous) => ({
                            ...previous,
                            [team]: parseOptionalNumber(event.target.value),
                          }))
                        }
                      />
                    </th>
                  ))}
                </tr>
                <tr>
                  <th rowSpan={2} aria-label="actions"><span className="col-resize-handle" onMouseDown={(e) => handleColResizeStart('actions', e)} /></th>
                  <th rowSpan={2}>ID<span className="col-resize-handle" onMouseDown={(e) => handleColResizeStart('id', e)} /></th>
                  <th rowSpan={2}>Project / Phase<span className="col-resize-handle" onMouseDown={(e) => handleColResizeStart('name', e)} /></th>
                  <th rowSpan={2}>Deps<span className="col-resize-handle" onMouseDown={(e) => handleColResizeStart('deps', e)} /></th>
                  <th rowSpan={2}>Value<span className="col-resize-handle" onMouseDown={(e) => handleColResizeStart('value', e)} /></th>
                  <th rowSpan={2}>Duration<span className="col-resize-handle" onMouseDown={(e) => handleColResizeStart('duration', e)} /></th>
                  <th colSpan={disciplines.length} className="discipline-header-group">
                    <span>FTE Allocation by Discipline</span>
                    <button
                      type="button"
                      className="add-discipline-btn"
                      title="Add discipline column"
                      onClick={() => {
                        const newName = `D${disciplines.length + 1}`
                        setDisciplines((prev) => [...prev, newName])
                        setTeamCapacities((prev) => ({ ...prev, [newName]: 1 }))
                        setLineItems((prev) =>
                          prev.map((item) => ({
                            ...item,
                            resourceAllocation: { ...item.resourceAllocation, [newName]: null },
                          })),
                        )
                      }}
                    >
                      +
                    </button>
                  </th>
                </tr>
                <tr>
                  {disciplines.map((team) => (
                    <th key={team} className="discipline-col-header">
                      <div className="discipline-header-cell">
                        <input
                          className="discipline-name-input"
                          value={team}
                          title="Rename discipline"
                          onChange={(event) => {
                            const newName = event.target.value
                            const oldName = team
                            setDisciplines((prev) => prev.map((d) => (d === oldName ? newName : d)))
                            setTeamCapacities((prev) => {
                              const next = { ...prev }
                              next[newName] = next[oldName]
                              if (newName !== oldName) delete next[oldName]
                              return next
                            })
                            setLineItems((prev) =>
                              prev.map((item) => {
                                const alloc = { ...item.resourceAllocation }
                                alloc[newName] = alloc[oldName]
                                if (newName !== oldName) delete alloc[oldName]
                                return { ...item, resourceAllocation: alloc }
                              }),
                            )
                            setColWidths((prev) => {
                              const next = { ...prev }
                              next[`fte-${newName}`] = next[`fte-${oldName}`] ?? 58
                              if (newName !== oldName) delete next[`fte-${oldName}`]
                              return next
                            })
                          }}
                        />
                        <button
                          type="button"
                          className="delete-discipline-btn"
                          title={`Delete ${team} column`}
                          onClick={() => {
                            if (disciplines.length <= 1) return
                            setDisciplines((prev) => prev.filter((d) => d !== team))
                            setTeamCapacities((prev) => {
                              const next = { ...prev }
                              delete next[team]
                              return next
                            })
                            setLineItems((prev) =>
                              prev.map((item) => {
                                const alloc = { ...item.resourceAllocation }
                                delete alloc[team]
                                return { ...item, resourceAllocation: alloc }
                              }),
                            )
                          }}
                        >
                          🗑
                        </button>
                      </div>
                      <span className="col-resize-handle" onMouseDown={(e) => handleColResizeStart(`fte-${team}`, e)} />
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {visibleRows.length === 0 ? (
                  <tr>
                    <td colSpan={6 + disciplines.length} className="empty-state">
                      Add a project to get started.
                    </td>
                  </tr>
                ) : (
                  visibleRows.map((item) => {
                    const dependenciesText =
                      dependencyDrafts[item.uid] ?? item.dependencyUids.join(', ')
                    const isProject = isProjectRow(item)
                    const childCount = lineItems.filter(
                      (candidate) => candidate.parentProjectUid === item.uid,
                    ).length
                    const isCollapsed = collapsedProjects.has(item.uid)
                    const isCalculatedProjectDuration = isProject && childCount > 0
                    const projectDuration = projectDurationByUid.get(item.uid) ?? 0
                    const dragItem = rowDragUid !== null ? lineItems.find((li) => li.uid === rowDragUid) : null
                    const isDragSource = rowDragUid !== null && (
                      rowDragUid === item.uid ||
                      // When dragging a project, also highlight its child phases
                      (dragItem && dragItem.parentProjectUid === null && item.parentProjectUid === rowDragUid)
                    )
                    const isDropTarget = rowDropTargetUid === item.uid && rowDragUid !== item.uid

                    return (
                      <tr
                        key={item.uid}
                        className={
                          (isProject ? 'project-row' : 'phase-row') +
                          (isDragSource ? ' drag-source' : '') +
                          (isDropTarget ? ' drop-target' : '')
                        }
                        onDragOver={(event) => handleRowDragOver(event, item.uid)}
                        onDrop={(event) => handleRowDrop(event, item.uid)}
                        onDragEnd={handleRowDragEnd}
                      >
                        <td className="actions-cell">
                          <span
                            className="drag-handle"
                            title="Drag to reorder"
                            draggable
                            onDragStart={(event) => {
                              event.stopPropagation()
                              handleRowDragStart(event as unknown as React.DragEvent<HTMLTableRowElement>, item.uid)
                            }}
                          >⠿</span>
                          <button
                            className="icon-delete"
                            onClick={() => removeRow(item.uid)}
                            type="button"
                            aria-label={`Delete row ${item.uid}`}
                            title="Delete row"
                          >
                            🗑
                          </button>
                        </td>
                        <td className="uid-cell">{item.uid}</td>
                        <td>
                          <div className={isProject ? 'name-cell project' : 'name-cell phase'}>
                            {isProject ? (
                              <button
                                type="button"
                                className="collapse-btn"
                                onClick={() => toggleProjectCollapsed(item.uid)}
                                disabled={childCount === 0}
                                aria-label={
                                  isCollapsed
                                    ? `Expand project ${item.uid}`
                                    : `Collapse project ${item.uid}`
                                }
                              >
                                {isCollapsed ? '▸' : '▾'}
                              </button>
                            ) : (
                              <span className="phase-bullet">↳</span>
                            )}
                            <input
                              aria-label={`${item.uid} name`}
                              ref={(element) => {
                                nameInputRefs.current[item.uid] = element
                              }}
                              value={item.name}
                              onChange={(event) => updateLineItem(item.uid, 'name', event.target.value)}
                              onKeyDown={(event) => handleHierarchyKeyDown(event, item)}
                              placeholder={isProject ? 'Project name' : 'Phase name'}
                            />
                          </div>
                        </td>
                        <td>
                          <input
                            aria-label={`${item.uid} dependencies`}
                            value={dependenciesText}
                            onChange={(event) => {
                              const rawValue = event.target.value
                              setDependencyDrafts((previous) => ({
                                ...previous,
                                [item.uid]: rawValue,
                              }))

                              const parsed = rawValue
                                .split(',')
                                .map((value) => value.trim())
                                .filter((value) => value !== '')
                                .map((value) => Number(value))
                                .filter((value) => Number.isFinite(value) && value > 0 && value !== item.uid)

                              updateLineItem(item.uid, 'dependencyUids', Array.from(new Set(parsed)))
                            }}
                            onBlur={() => {
                              setDependencyDrafts((previous) => {
                                const next = { ...previous }
                                delete next[item.uid]
                                return next
                              })
                            }}
                          />
                        </td>
                        <td className="numeric-cell">
                          {isProject ? (
                            <input
                              aria-label={`${item.uid} value score`}
                              type="number"
                              min={0}
                              value={toNumberInputValue(item.valueScore)}
                              placeholder="Score"
                              onChange={(event) =>
                                updateLineItem(item.uid, 'valueScore', parseOptionalNumber(event.target.value))
                              }
                            />
                          ) : (
                            <span className="phase-readonly">—</span>
                          )}
                        </td>
                        <td className="numeric-cell">
                          <input
                            aria-label={`${item.uid} duration`}
                            type="number"
                            min={0}
                            value={
                              isCalculatedProjectDuration
                                ? projectDuration
                                : toNumberInputValue(item.durationMonths)
                            }
                            placeholder="Months"
                            onChange={(event) =>
                              updateLineItem(item.uid, 'durationMonths', parseOptionalNumber(event.target.value))
                            }
                            readOnly={isCalculatedProjectDuration}
                            className={isCalculatedProjectDuration ? 'calculated-field' : ''}
                          />
                        </td>
                        {disciplines.map((team) => (
                          <td key={`${item.uid}-${team}`} className="numeric-cell">
                            {isProject && childCount > 0 ? null : (
                              <input
                                aria-label={`${item.uid} ${team} fte`}
                                type="number"
                                min={0}
                                step="0.1"
                                value={toNumberInputValue(item.resourceAllocation[team])}
                                placeholder="FTE"
                                onChange={(event) =>
                                  updateAllocation(item.uid, team, parseOptionalNumber(event.target.value))
                                }
                              />
                            )}
                          </td>
                        ))}
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </section>
      ) : (
        <section className="tab-content scheduler-tab">
          <div className="toolbar">
            <button onClick={() => setPixelsPerMonth((value) => Math.max(18, value - 6))} type="button">
              Zoom Out
            </button>
            <button onClick={() => setPixelsPerMonth((value) => Math.min(80, value + 6))} type="button">
              Zoom In
            </button>
            <button onClick={handleSchedulerUndo} type="button" disabled={schedulerUndoStack.length === 0}>
              Undo
            </button>
            <button onClick={handleSchedulerRedo} type="button" disabled={schedulerRedoStack.length === 0}>
              Redo
            </button>
            <button onClick={handleRecalculateAroundFrozen} type="button">
              Rebalance
            </button>
            <button onClick={handleResetScheduler} type="button">
              Reset
            </button>
            <button onClick={handleSave} type="button">
              Save File
            </button>
            <button onClick={handleLoad} type="button">
              Load File
            </button>
            <button onClick={handleCopyShareLink} type="button">
              Copy Share Link
            </button>
            <span className="toolbar-separator" />
            <div className="toolbar-toggle">
              <span className="toolbar-toggle-label">Time:</span>
              {(['auto', 'monthly', 'quarterly'] as const).map((mode) => (
                <button
                  key={mode}
                  type="button"
                  className={timeGranularity === mode ? 'toggle-btn active' : 'toggle-btn'}
                  onClick={() => setTimeGranularity(mode)}
                >
                  {mode === 'auto' ? 'Auto' : mode === 'monthly' ? 'Monthly' : 'Quarterly'}
                </button>
              ))}
            </div>
            <span className="toolbar-separator" />
            <button
              type="button"
              className="icon-btn"
              title="Expand all projects"
              onClick={() => setCollapsedProjectsInScheduler(new Set())}
            >
              ⊞
            </button>
            <button
              type="button"
              className="icon-btn"
              title="Collapse all projects"
              onClick={() => {
                const allGroupIds = lineItems
                  .filter((item) => isProjectRow(item) && lineItems.some((c) => c.parentProjectUid === item.uid))
                  .map((item) => item.uid)
                setCollapsedProjectsInScheduler(new Set(allGroupIds))
              }}
            >
              ⊟
            </button>
            <span className="hint">Drag bars to freeze positions and rebalance remaining work. Right-click a bar to unfreeze it.</span>
          </div>

          <div className="scheduler-scroll">
            <div className="gantt-header" style={{ width: schedulerCanvasWidth }}>
              <div className="gantt-label-header" style={{ width: schedulerLabelWidth }}>
                Activity (Priority)
                <span className="scheduler-col-resize-handle" onMouseDown={handleSchedulerLabelResizeStart} />
              </div>
              <div className="gantt-calendar" style={{ width: timelineWidth }}>
                <div className="calendar-year-row">
                  {yearSpans.map((span, idx) => (
                    <div
                      key={`year-${idx}`}
                      className="year-cell"
                      style={{ width: span.count * calendarUnitWidth }}
                    >
                      {span.year}
                    </div>
                  ))}
                </div>
                <div className="calendar-time-row">
                  {timeLabels.map((label, index) => (
                    <div
                      key={`time-${index}`}
                      className="time-cell"
                      style={{ width: calendarUnitWidth }}
                    >
                      {label}
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div className="gantt-body" style={{ width: schedulerCanvasWidth }}>
              <div className="gantt-gridlines" style={{ left: schedulerLabelWidth, width: timelineWidth }}>
                {showQuarters
                  ? Array.from({ length: calendarTimeUnits }, (_, i) => {
                      const monthIndex = i * 3
                      const d = new Date(timelineStart)
                      d.setMonth(d.getMonth() + monthIndex + 3)
                      const isYearBoundary = d.getMonth() === 0
                      return (
                        <div
                          key={`grid-q-${i}`}
                          className={isYearBoundary ? 'gantt-gridline year-boundary' : 'gantt-gridline quarter-boundary'}
                          style={{ width: pixelsPerMonth * 3 }}
                        />
                      )
                    })
                  : Array.from({ length: displayMonths }, (_, i) => {
                      const d = new Date(timelineStart)
                      d.setMonth(d.getMonth() + i + 1)
                      const isYearBoundary = d.getMonth() === 0
                      const isQuarterBoundary = d.getMonth() % 3 === 0
                      const cls = isYearBoundary
                        ? 'gantt-gridline year-boundary'
                        : isQuarterBoundary
                          ? 'gantt-gridline quarter-boundary'
                          : 'gantt-gridline'
                      return <div key={`grid-m-${i}`} className={cls} style={{ width: pixelsPerMonth }} />
                    })}
              </div>
              <div
                className="today-line"
                style={{ left: `${schedulerLabelWidth + Math.max(0, todayMonthFloat * pixelsPerMonth)}px` }}
              />
              {taskGroups.map((group) => {
                const isCollapsed = collapsedProjectsInScheduler.has(group.projectId)
                const hasPhases = group.phaseTasks.length > 0

                return (
                  <div key={`group-${group.projectId}`} className="gantt-project-group">
                    {group.projectTask && (
                      <div className="gantt-row project-row" style={{ width: schedulerCanvasWidth }}>
                        <div className="gantt-row-label" style={{ width: schedulerLabelWidth }}>
                          {hasPhases ? (
                            <button
                              className="expand-collapse-btn"
                              onClick={() => {
                                const newSet = new Set(collapsedProjectsInScheduler)
                                if (isCollapsed) {
                                  newSet.delete(group.projectId)
                                } else {
                                  newSet.add(group.projectId)
                                }
                                setCollapsedProjectsInScheduler(newSet)
                              }}
                              type="button"
                            >
                              {isCollapsed ? '▶' : '▼'}
                            </button>
                          ) : (
                            <span className="expand-spacer" />
                          )}
                          <strong>{group.projectTask.name} ({group.projectTask.priority})</strong>
                        </div>
                        <div className="gantt-row-track" style={{ width: timelineWidth }}>
                          <div
                            className={
                              group.projectTask.frozen ? 'gantt-bar project-bar frozen' : 'gantt-bar project-bar'
                            }
                            style={{
                              left: group.projectTask.startMonth * pixelsPerMonth,
                              width: Math.max(6, group.projectTask.duration * pixelsPerMonth),
                            }}
                            onMouseDown={(event) => {
                              setDragTaskId(group.projectTask!.id)
                              setDragStartX(event.clientX)
                              setDragInitialMonth(group.projectTask!.startMonth)
                              setDragBaseTasks(schedulerData.tasks)
                              setDragBaseOverrides(cloneOverrides(schedulerOverrides))
                              setDragPreviewOverrides(schedulerOverrides)
                            }}
                            onContextMenu={(event) => {
                              event.preventDefault()
                              const taskId = group.projectTask!.id
                              const phaseIds = group.phaseTasks.map((p) => p.id)
                              const allIds = [taskId, ...phaseIds]
                              const anyFrozen = allIds.some((id) => schedulerOverrides[id]?.frozen)
                              if (anyFrozen) {
                                pushSchedulerHistory(schedulerOverrides)
                                setSchedulerOverrides((prev) => {
                                  const next = cloneOverrides(prev)
                                  allIds.forEach((id) => {
                                    if (next[id]) next[id] = { ...next[id], frozen: false }
                                  })
                                  return next
                                })
                                setStatusMessage('Project and all phases unfrozen. They will move on next rebalance.')
                              }
                            }}
                            title={group.projectTask.frozen ? 'Frozen — right-click to unfreeze' : 'Scheduled — drag to freeze position'}
                          >
                            {group.projectTask.duration}m
                          </div>
                        </div>
                      </div>
                    )}
                    {!isCollapsed && hasPhases && (
                      <div className="phase-container">
                        {group.phaseTasks.map((phaseTask) => {
                          const left = phaseTask.startMonth * pixelsPerMonth
                          const width = Math.max(6, phaseTask.duration * pixelsPerMonth)

                          return (
                            <div key={`row-${phaseTask.id}`} className="gantt-row phase-row" style={{ width: schedulerCanvasWidth }}>
                              <div className="gantt-row-label phase-label" style={{ width: schedulerLabelWidth }}>
                                <span className="phase-indent">└</span>
                                {phaseTask.name}
                              </div>
                              <div className="gantt-row-track" style={{ width: timelineWidth }}>
                                <div
                                  className={phaseTask.frozen ? 'gantt-bar phase-bar frozen' : 'gantt-bar phase-bar'}
                                  style={{ left, width }}
                                  onMouseDown={(event) => {
                                    setDragTaskId(phaseTask.id)
                                    setDragStartX(event.clientX)
                                    setDragInitialMonth(phaseTask.startMonth)
                                    setDragBaseTasks(schedulerData.tasks)
                                    setDragBaseOverrides(cloneOverrides(schedulerOverrides))
                                    setDragPreviewOverrides(schedulerOverrides)
                                  }}
                                  onContextMenu={(event) => {
                                    event.preventDefault()
                                    const taskId = phaseTask.id
                                    if (schedulerOverrides[taskId]?.frozen) {
                                      pushSchedulerHistory(schedulerOverrides)
                                      setSchedulerOverrides((prev) => {
                                        const next = cloneOverrides(prev)
                                        next[taskId] = { ...next[taskId], frozen: false }
                                        return next
                                      })
                                      setStatusMessage('Task unfrozen. It will move on next rebalance.')
                                    }
                                  }}
                                  title={phaseTask.frozen ? 'Frozen — right-click to unfreeze' : 'Scheduled — drag to freeze position'}
                                >
                                  {phaseTask.duration}m
                                </div>
                              </div>
                            </div>
                          )
                        })}
                      </div>
                    )}
                  </div>
                )
              })}
            </div>

            <div className="histogram" style={{ width: schedulerCanvasWidth }}>
              <div className="histogram-toggle">
                <button
                  type="button"
                  className={histogramMode === 'separate' ? 'toggle-btn active' : 'toggle-btn'}
                  onClick={() => setHistogramMode('separate')}
                >
                  Separate
                </button>
                <button
                  type="button"
                  className={histogramMode === 'combined' ? 'toggle-btn active' : 'toggle-btn'}
                  onClick={() => setHistogramMode('combined')}
                >
                  Combined
                </button>
              </div>
              {histogramMode === 'separate' ? (
                disciplines.map((team) => (
                  <div key={`hist-${team}`} className="histogram-row" style={{ width: schedulerCanvasWidth }}>
                    <div className="histogram-label" style={{ width: schedulerLabelWidth }}>{team}</div>
                    <div className="histogram-bars" style={{ width: timelineWidth, position: 'relative' }}>
                      {histogramBuckets.map((bucket) => {
                        const values = (displayUtilization.utilizationByTeam[team] ?? []).slice(
                          bucket.bucketStart,
                          bucket.bucketEnd,
                        )
                        const average =
                          values.length === 0
                            ? 0
                            : values.reduce((sum, value) => sum + value, 0) / values.length
                        const capped = Math.min(160, average)

                        return (
                          <div
                            key={`${team}-${bucket.bucketStart}`}
                            className="histogram-bar-wrap"
                            style={{ width: (bucket.bucketEnd - bucket.bucketStart) * pixelsPerMonth }}
                            title={`${team}: ${average.toFixed(0)}%`}
                          >
                            <div
                              className={average > 100 ? 'histogram-bar over' : 'histogram-bar'}
                              style={{ height: `${Math.max(2, capped)}%` }}
                            />
                          </div>
                        )
                      })}
                    </div>
                  </div>
                ))
              ) : (
                <div className="histogram-row combined-row" style={{ width: schedulerCanvasWidth }}>
                  <div className="histogram-label" style={{ width: schedulerLabelWidth }}>
                    <div className="combined-legend">
                      {disciplines.map((team) => (
                        <span key={team} className="legend-item">
                          <span className="legend-swatch" style={{ background: getDisciplineColor(disciplines, team) }} />
                          {team}
                        </span>
                      ))}
                    </div>
                  </div>
                  <div className="histogram-bars combined-bars" style={{ width: timelineWidth, position: 'relative' }}>
                    <div className="capacity-line" />
                    {histogramBuckets.map((bucket) => {
                      const bucketWidth = (bucket.bucketEnd - bucket.bucketStart) * pixelsPerMonth
                      const barWidth = Math.max(1, (bucketWidth - 2) / disciplines.length)

                      return (
                        <div
                          key={`combined-${bucket.bucketStart}`}
                          className="histogram-bar-wrap combined-wrap"
                          style={{ width: bucketWidth }}
                        >
                          {disciplines.map((team) => {
                            const values = (displayUtilization.utilizationByTeam[team] ?? []).slice(
                              bucket.bucketStart,
                              bucket.bucketEnd,
                            )
                            const average =
                              values.length === 0
                                ? 0
                                : values.reduce((sum, value) => sum + value, 0) / values.length
                            const capped = Math.min(160, average)

                            return (
                              <div
                                key={`${team}-${bucket.bucketStart}`}
                                className={average > 100 ? 'combined-bar over' : 'combined-bar'}
                                style={{
                                  width: barWidth,
                                  height: `${Math.max(1, capped)}%`,
                                  background: average > 100 ? '#d87474' : getDisciplineColor(disciplines, team),
                                }}
                                title={`${team}: ${average.toFixed(0)}% of capacity`}
                              />
                            )
                          })}
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}
            </div>
          </div>
        </section>
      )}
      <input
        type="file"
        accept=".json,application/json"
        className="hidden-input"
        ref={loadFileInputRef}
        onChange={handleLoadFile}
      />
    </div>
  )
}

export default App
