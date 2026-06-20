import { useCallback, useEffect, useMemo, useState } from 'react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Codicon } from '@/components/ui/codicon'
import { SearchField } from '@/components/ui/search-field'
import { Switch } from '@/components/ui/switch'
import { getSkills, toggleSkill } from '@/hermes'
import { useI18n } from '@/i18n'
import { Check, Sparkles } from '@/lib/icons'
import { cn } from '@/lib/utils'
import { notify, notifyError } from '@/store/notifications'
import type { SkillInfo } from '@/types/hermes'

import { asText, includesQuery, prettyName } from './helpers'
import { LoadingState, Pill, SectionHeading, SettingsContent } from './primitives'

function categoryFor(skill: SkillInfo): string {
  return asText(skill.category) || 'general'
}

function filteredSkills(skills: SkillInfo[], query: string, category: string | null): SkillInfo[] {
  const q = query.trim().toLowerCase()

  return skills
    .filter(skill => {
      if (category && categoryFor(skill) !== category) {
        return false
      }

      if (!q) {
        return true
      }

      return includesQuery(skill.name, q) || includesQuery(skill.description, q) || includesQuery(skill.category, q)
    })
    .sort((a, b) => asText(a.name).localeCompare(asText(b.name)))
}

export function SkillToggleSettings() {
  const { t } = useI18n()
  const [skills, setSkills] = useState<SkillInfo[] | null>(null)
  const [query, setQuery] = useState('')
  const [activeCategory, setActiveCategory] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const [savingSkill, setSavingSkill] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    setRefreshing(true)

    try {
      setSkills(await getSkills())
    } catch (err) {
      notifyError(err, t.skills.skillsLoadFailed)
    } finally {
      setRefreshing(false)
    }
  }, [t])

  useEffect(() => {
    void refresh()
  }, [refresh])

  const categories = useMemo(() => {
    if (!skills) {
      return []
    }

    const counts = new Map<string, number>()

    for (const skill of skills) {
      const key = categoryFor(skill)
      counts.set(key, (counts.get(key) || 0) + 1)
    }

    return Array.from(counts.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, count]) => ({ key, count }))
  }, [skills])

  const visibleSkills = useMemo(
    () => (skills ? filteredSkills(skills, query, activeCategory) : []),
    [activeCategory, query, skills]
  )

  const totalSkills = skills?.length || 0
  const enabledSkills = skills?.filter(skill => skill.enabled).length || 0

  async function handleToggleSkill(skill: SkillInfo, enabled: boolean) {
    setSavingSkill(skill.name)

    try {
      await toggleSkill(skill.name, enabled)
      setSkills(current => current?.map(row => (row.name === skill.name ? { ...row, enabled } : row)) ?? current)
      notify({
        kind: 'success',
        title: enabled ? t.skills.skillEnabled : t.skills.skillDisabled,
        message: t.skills.appliesToNewSessions(skill.name)
      })
    } catch (err) {
      notifyError(err, t.skills.failedToUpdate(skill.name))
    } finally {
      setSavingSkill(null)
    }
  }

  return (
    <SettingsContent>
      <div className="grid gap-4 py-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <SectionHeading icon={Sparkles} meta={`${enabledSkills}/${totalSkills}`} title={t.settings.nav.skills} />
            <p className="max-w-2xl text-[length:var(--conversation-caption-font-size)] leading-(--conversation-caption-line-height) text-(--ui-text-tertiary)">
              {t.skills.appliesToNewSessions(t.skills.tabSkills)}
            </p>
          </div>
          <SearchField
            aria-label={t.skills.searchSkills}
            containerClassName="w-full sm:w-auto"
            loading={refreshing}
            onChange={setQuery}
            placeholder={t.skills.searchSkills}
            trailingAction={
              <Button
                aria-label={refreshing ? t.skills.refreshing : t.skills.refresh}
                disabled={refreshing}
                onClick={() => void refresh()}
                size="icon-xs"
                title={refreshing ? t.skills.refreshing : t.skills.refresh}
                type="button"
                variant="ghost"
              >
                <Codicon name="refresh" size="0.875rem" spinning={refreshing} />
              </Button>
            }
            value={query}
          />
        </div>

        {categories.length > 0 && (
          <div className="flex flex-wrap items-center gap-1.5">
            <CategoryButton active={activeCategory === null} count={totalSkills} label={t.skills.all} onClick={() => setActiveCategory(null)} />
            {categories.map(category => (
              <CategoryButton
                active={activeCategory === category.key}
                count={category.count}
                key={category.key}
                label={prettyName(category.key)}
                onClick={() => setActiveCategory(activeCategory === category.key ? null : category.key)}
              />
            ))}
          </div>
        )}

        {!skills ? (
          <LoadingState label={t.skills.loading} />
        ) : visibleSkills.length === 0 ? (
          <EmptyState description={t.skills.noSkillsDesc} title={t.skills.noSkillsTitle} />
        ) : (
          <div className="divide-y divide-border/35 rounded-xl bg-background/45">
            {visibleSkills.map(skill => (
              <div
                className="grid gap-3 px-3 py-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center"
                key={skill.name}
              >
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <div className="truncate text-sm font-medium">{skill.name}</div>
                    <Pill tone={skill.enabled ? 'primary' : 'muted'}>
                      {skill.enabled && <Check className="size-3" />}
                      {skill.enabled ? t.common.on : t.common.off}
                    </Pill>
                    <Badge className="bg-(--ui-bg-quinary) text-(--ui-text-tertiary)">{prettyName(categoryFor(skill))}</Badge>
                  </div>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {asText(skill.description) || t.skills.noDescription}
                  </p>
                </div>
                <Switch
                  aria-label={`Toggle ${skill.name} skill`}
                  checked={skill.enabled}
                  disabled={savingSkill === skill.name}
                  onCheckedChange={checked => void handleToggleSkill(skill, checked)}
                />
              </div>
            ))}
          </div>
        )}
      </div>
    </SettingsContent>
  )
}

function CategoryButton({
  active,
  count,
  label,
  onClick
}: {
  active: boolean
  count: number
  label: string
  onClick: () => void
}) {
  return (
    <button
      className={cn(
        'inline-flex min-h-7 items-center gap-1.5 rounded-md px-2 text-xs transition',
        active
          ? 'bg-(--ui-bg-tertiary) text-foreground'
          : 'text-(--ui-text-secondary) hover:bg-(--chrome-action-hover) hover:text-foreground'
      )}
      onClick={onClick}
      type="button"
    >
      <span>{label}</span>
      <span className="text-[0.68rem] text-muted-foreground">{count}</span>
    </button>
  )
}

function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="grid min-h-48 place-items-center rounded-xl bg-background/45 text-center">
      <div>
        <div className="text-sm font-medium">{title}</div>
        <div className="mt-1 text-xs text-muted-foreground">{description}</div>
      </div>
    </div>
  )
}
