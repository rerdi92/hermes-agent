import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import {
  $paneHeightOverride,
  $paneOpen,
  $paneStates,
  $paneWidthOverride,
  clearAllPaneSizeOverrides,
  clearPaneHeightOverride,
  clearPaneWidthOverride,
  ensurePaneRegistered,
  getPaneStateSnapshot,
  setPaneHeightOverride,
  setPaneOpen,
  setPaneWidthOverride,
  togglePane
} from './panes'

const STORAGE_KEY = 'hermes.desktop.paneStates.v1'

describe('panes store', () => {
  beforeEach(() => {
    $paneStates.set({})
    window.localStorage.clear()
  })

  afterEach(() => {
    $paneStates.set({})
    window.localStorage.clear()
  })

  describe('ensurePaneRegistered', () => {
    it('adds a pane with defaults when missing', () => {
      ensurePaneRegistered('files', { open: true })

      expect(getPaneStateSnapshot('files')).toEqual({ open: true, widthOverride: undefined })
    })

    it('is a no-op when the pane already exists', () => {
      ensurePaneRegistered('files', { open: false })
      ensurePaneRegistered('files', { open: true })

      expect(getPaneStateSnapshot('files')?.open).toBe(false)
    })

    it('preserves an existing widthOverride when re-registering', () => {
      ensurePaneRegistered('files', { open: true })
      setPaneWidthOverride('files', 360)
      ensurePaneRegistered('files', { open: false })

      expect(getPaneStateSnapshot('files')?.widthOverride).toBe(360)
    })
  })

  describe('setPaneOpen / togglePane', () => {
    it('updates the pane open flag', () => {
      ensurePaneRegistered('files', { open: false })
      setPaneOpen('files', true)

      expect(getPaneStateSnapshot('files')?.open).toBe(true)
    })

    it('togglePane flips the current value', () => {
      ensurePaneRegistered('files', { open: false })
      togglePane('files')
      togglePane('files')
      togglePane('files')

      expect(getPaneStateSnapshot('files')?.open).toBe(true)
    })

    it('togglePane on an unregistered id starts from false', () => {
      togglePane('ephemeral')

      expect(getPaneStateSnapshot('ephemeral')?.open).toBe(true)
    })

    it('preserves width and height overrides across open/close changes', () => {
      ensurePaneRegistered('files', { open: true })
      setPaneWidthOverride('files', 280)
      setPaneHeightOverride('files', 320)
      setPaneOpen('files', false)
      setPaneOpen('files', true)

      expect(getPaneStateSnapshot('files')?.widthOverride).toBe(280)
      expect(getPaneStateSnapshot('files')?.heightOverride).toBe(320)
    })
  })

  describe('size overrides', () => {
    it('stores width and height values in pixels', () => {
      ensurePaneRegistered('files', { open: true })
      setPaneWidthOverride('files', 300)
      setPaneHeightOverride('files', 420)

      expect(getPaneStateSnapshot('files')?.widthOverride).toBe(300)
      expect(getPaneStateSnapshot('files')?.heightOverride).toBe(420)
    })

    it('clears individual width and height overrides', () => {
      ensurePaneRegistered('files', { open: true })
      setPaneWidthOverride('files', 300)
      setPaneHeightOverride('files', 420)
      clearPaneWidthOverride('files')
      clearPaneHeightOverride('files')

      expect(getPaneStateSnapshot('files')?.widthOverride).toBeUndefined()
      expect(getPaneStateSnapshot('files')?.heightOverride).toBeUndefined()
    })

    it('persists width and height overrides with the pane state', () => {
      ensurePaneRegistered('files', { open: true })
      setPaneWidthOverride('files', 300)
      setPaneHeightOverride('files', 420)

      const persisted = window.localStorage.getItem(STORAGE_KEY)

      expect(persisted).not.toBeNull()
      expect(JSON.parse(persisted ?? '{}')).toEqual({
        files: { heightOverride: 420, open: true, widthOverride: 300 }
      })
    })

    it('clears every size override without changing pane open state', () => {
      ensurePaneRegistered('files', { open: true })
      ensurePaneRegistered('terminal', { open: false })
      setPaneWidthOverride('files', 300)
      setPaneHeightOverride('terminal', 420)

      clearAllPaneSizeOverrides()

      expect($paneStates.get()).toEqual({ files: { open: true }, terminal: { open: false } })
    })

    it('open flag is persisted across changes', () => {
      ensurePaneRegistered('files', { open: false })
      setPaneOpen('files', true)

      const persisted = window.localStorage.getItem(STORAGE_KEY)

      expect(persisted).not.toBeNull()
      expect(JSON.parse(persisted ?? '{}')).toEqual({ files: { open: true } })
    })
  })

  describe('derived atoms', () => {
    it('$paneOpen reflects the pane state', () => {
      const open$ = $paneOpen('files')
      expect(open$.get()).toBe(false)

      ensurePaneRegistered('files', { open: true })
      expect(open$.get()).toBe(true)

      setPaneOpen('files', false)
      expect(open$.get()).toBe(false)
    })

    it('size override atoms reflect width and height', () => {
      const width$ = $paneWidthOverride('files')
      const height$ = $paneHeightOverride('files')
      expect(width$.get()).toBeUndefined()
      expect(height$.get()).toBeUndefined()

      ensurePaneRegistered('files', { open: true })
      setPaneWidthOverride('files', 240)
      setPaneHeightOverride('files', 360)
      expect(width$.get()).toBe(240)
      expect(height$.get()).toBe(360)
    })

    it('$paneOpen returns the same atom instance for repeated calls', () => {
      expect($paneOpen('files')).toBe($paneOpen('files'))
    })
  })
})
