import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const getSkills = vi.fn()
const toggleSkill = vi.fn()

vi.mock('@/hermes', () => ({
  getSkills: () => getSkills(),
  toggleSkill: (name: string, enabled: boolean) => toggleSkill(name, enabled)
}))

vi.mock('@/store/notifications', () => ({
  notify: vi.fn(),
  notifyError: vi.fn()
}))

function skill(overrides: Record<string, unknown> = {}) {
  return {
    name: 'ultrawork',
    category: 'autonomous-ai-agents',
    description: 'End-to-end execution mode',
    enabled: true,
    ...overrides
  }
}

async function renderSkillToggleSettings() {
  const { SkillToggleSettings } = await import('./skill-toggle-settings')

  return render(<SkillToggleSettings />)
}

beforeEach(() => {
  getSkills.mockResolvedValue([
    skill(),
    skill({ name: 'ultraresearch', category: 'research', description: 'Deep research mode', enabled: false })
  ])
  toggleSkill.mockResolvedValue({ ok: true, name: 'ultraresearch', enabled: true })
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

describe('SkillToggleSettings', () => {
  it('renders all skills including disabled skills', async () => {
    await renderSkillToggleSettings()

    expect(await screen.findByText('ultrawork')).toBeTruthy()
    expect(await screen.findByText('ultraresearch')).toBeTruthy()
    expect(screen.getByRole('switch', { name: 'Toggle ultraresearch skill' }).getAttribute('aria-checked')).toBe(
      'false'
    )
  })

  it('enables a disabled skill from the Settings tab', async () => {
    await renderSkillToggleSettings()

    fireEvent.click(await screen.findByRole('switch', { name: 'Toggle ultraresearch skill' }))

    await waitFor(() => expect(toggleSkill).toHaveBeenCalledWith('ultraresearch', true))
    await waitFor(() =>
      expect(screen.getByRole('switch', { name: 'Toggle ultraresearch skill' }).getAttribute('aria-checked')).toBe(
        'true'
      )
    )
  })

  it('filters skills by category tab', async () => {
    await renderSkillToggleSettings()

    await screen.findByText('ultraresearch')

    const categoryButton = screen.getAllByText('Research')[0].closest('button')
    expect(categoryButton).toBeTruthy()
    fireEvent.click(categoryButton!)

    expect(screen.queryByText('ultrawork')).toBeNull()
    expect(screen.getByText('ultraresearch')).toBeTruthy()
  })
})
