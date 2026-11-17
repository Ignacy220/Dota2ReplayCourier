package parser

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dotabuff/manta"
	"github.com/dotabuff/manta/dota"
)

// BoilerplateMantaParser is the built-in rich in-RAM default parser ("default") built on dotabuff/manta.
type BoilerplateMantaParser struct{}

// NewBoilerplateMantaParser creates the default built-in rich Manta replay parser.
func NewBoilerplateMantaParser() *BoilerplateMantaParser {
	return &BoilerplateMantaParser{}
}

func (p *BoilerplateMantaParser) Name() string {
	return "default"
}

func (p *BoilerplateMantaParser) Version() string {
	return "2.0.0"
}

// DraftPickBan records a hero pick or ban during match drafting.
type DraftPickBan struct {
	IsPick bool   `json:"is_pick"`
	Team   uint32 `json:"team"`
	HeroID int32  `json:"hero_id"`
}

// PlayerSample captures per-minute progression and map position for a hero.
type PlayerSample struct {
	TimeSec  int    `json:"time_sec"`
	X        uint64 `json:"x,omitempty"`
	Y        uint64 `json:"y,omitempty"`
	Level    int32  `json:"level,omitempty"`
	Gold     int32  `json:"gold,omitempty"`
	NetWorth int32  `json:"net_worth,omitempty"`
	LastHits int32  `json:"lh,omitempty"`
	Denies   int32  `json:"dn,omitempty"`
	XP       int32  `json:"xp,omitempty"`
	Health   int32  `json:"hp,omitempty"`
}

// PlayerSummary captures full end-of-match stats, items, and timeline samples for a player.
type PlayerSummary struct {
	PlayerSlot   int            `json:"player_slot"`
	Team         string         `json:"team"`
	AccountID    uint64         `json:"account_id,omitempty"`
	PlayerName   string         `json:"player_name,omitempty"`
	HeroID       int32          `json:"hero_id,omitempty"`
	HeroName     string         `json:"hero_name,omitempty"`
	Level        int32          `json:"level,omitempty"`
	Kills        int32          `json:"kills"`
	Deaths       int32          `json:"deaths"`
	Assists      int32          `json:"assists"`
	LastHits     int32          `json:"last_hits"`
	Denies       int32          `json:"denies"`
	Gold         int32          `json:"gold,omitempty"`
	NetWorth     int32          `json:"net_worth,omitempty"`
	GPM          int            `json:"gpm,omitempty"`
	XPM          int            `json:"xpm,omitempty"`
	HeroDamage   uint32         `json:"hero_damage,omitempty"`
	TowerDamage  uint32         `json:"tower_damage,omitempty"`
	HeroHealing  uint32         `json:"hero_healing,omitempty"`
	ObsPlaced    int32          `json:"obs_placed,omitempty"`
	SenPlaced    int32          `json:"sen_placed,omitempty"`
	Items        []string       `json:"items,omitempty"`
	NeutralItem  string         `json:"neutral_item,omitempty"`
	MinuteSample []PlayerSample `json:"minute_samples,omitempty"`
}

// WardPlacement represents an observer or sentry ward placement or deward event.
type WardPlacement struct {
	TimeSec int    `json:"time"`
	X       int    `json:"x"`
	Y       int    `json:"y"`
	Type    string `json:"type"`
	Team    int    `json:"team"`
	Event   string `json:"event,omitempty"` // "placed" | "destroyed"
}

// CombatSummary represents aggregated combat metrics per hero.
type CombatSummary struct {
	HeroName    string `json:"hero_name"`
	DamageDealt uint32 `json:"damage_dealt"`
	HeroDamage  uint32 `json:"hero_damage"`
	Healing     uint32 `json:"healing"`
	Kills       int    `json:"kills"`
	Deaths      int    `json:"deaths"`
}

// CombatLogEvent represents a single decoded Source 2 combat log entry.
type CombatLogEvent struct {
	TimeSec        float32 `json:"time_sec"`
	Type           string  `json:"type"`
	Attacker       string  `json:"attacker,omitempty"`
	Target         string  `json:"target,omitempty"`
	Inflictor      string  `json:"inflictor,omitempty"`
	Value          uint32  `json:"value,omitempty"`
	StunDuration   float32 `json:"stun_duration,omitempty"`
	AttackerHero   bool    `json:"is_attacker_hero,omitempty"`
	TargetHero     bool    `json:"is_target_hero,omitempty"`
	AttackerIllus  bool    `json:"is_attacker_illusion,omitempty"`
	TargetIllusion bool    `json:"is_target_illusion,omitempty"`
}

// ObjectiveEvent captures towers, barracks, Roshan, Aegis, and buyback milestones.
type ObjectiveEvent struct {
	TimeSec float32 `json:"time_sec"`
	Type    string  `json:"type"` // "tower", "barracks", "roshan", "aegis", "buyback", "first_blood"
	Team    uint32  `json:"team,omitempty"`
	Unit    string  `json:"unit,omitempty"`
	Killer  string  `json:"killer,omitempty"`
	Player  int32   `json:"player_id,omitempty"`
}

// ChatEntry captures in-game text chat messages.
type ChatEntry struct {
	TimeSec  float32 `json:"time_sec"`
	PlayerID int32   `json:"player_id"`
	Channel  uint32  `json:"channel"`
	Text     string  `json:"text"`
}

// MapPing captures team map pings.
type MapPing struct {
	TimeSec  float32 `json:"time_sec"`
	PlayerID int32   `json:"player_id"`
	X        int32   `json:"x"`
	Y        int32   `json:"y"`
	Type     uint32  `json:"type"`
	Direct   bool    `json:"direct_ping,omitempty"`
}

// GameChatEvent captures system events (runes, courier kills, shrine/tormentor/etc.).
type GameChatEvent struct {
	TimeSec   float32 `json:"time_sec"`
	Type      string  `json:"type"`
	Value     uint32  `json:"value,omitempty"`
	PlayerID1 int32   `json:"playerid_1,omitempty"`
	PlayerID2 int32   `json:"playerid_2,omitempty"`
}

// BoilerplateResult is the rich OpenDota-style structured JSON payload emitted by the Default parser.
type BoilerplateResult struct {
	MatchID           uint64                   `json:"match_id"`
	Cluster           uint32                   `json:"cluster"`
	Region            string                   `json:"region"`
	DurationSeconds   int                      `json:"duration"`
	RadiantWin        bool                     `json:"radiant_win"`
	Winner            string                   `json:"winner"`
	MapName           string                   `json:"map_name,omitempty"`
	BuildNum          int32                    `json:"build_num,omitempty"`
	GameMode          int32                    `json:"game_mode,omitempty"`
	LeagueID          uint32                   `json:"league_id,omitempty"`
	RadiantTeamID     uint32                   `json:"radiant_team_id,omitempty"`
	DireTeamID        uint32                   `json:"dire_team_id,omitempty"`
	RadiantTeamTag    string                   `json:"radiant_team_tag,omitempty"`
	DireTeamTag       string                   `json:"dire_team_tag,omitempty"`
	EndTime           uint32                   `json:"end_time,omitempty"`
	TotalTicks        uint32                   `json:"total_ticks"`
	CompressedBytes   int                      `json:"compressed_bytes"`
	PicksBans         []DraftPickBan           `json:"picks_bans,omitempty"`
	Players           []PlayerSummary          `json:"players"`
	Wards             []WardPlacement          `json:"wards"`
	Combat            map[string]CombatSummary `json:"combat"`
	CombatLog         []CombatLogEvent         `json:"combat_log"`
	Objectives        []ObjectiveEvent         `json:"objectives"`
	Chat              []ChatEntry              `json:"chat"`
	Pings             []MapPing                `json:"pings"`
	GameEvents        []GameChatEvent          `json:"game_events"`
	ParsedInRAM       bool                     `json:"parsed_in_ram"`
	ParseTimeMs       int64                    `json:"parse_time_ms"`
	PartialEntityNote string                   `json:"partial_entity_note,omitempty"`
}

// Parse decompresses input.CompressedBz2 in memory and extracts comprehensive replay telemetry via Manta.
func (p *BoilerplateMantaParser) Parse(ctx context.Context, ctrl ExecutionController, input ParseInput) (*ParseOutput, error) {
	start := time.Now()
	if len(input.CompressedBz2) == 0 {
		return nil, fmt.Errorf("empty compressed replay buffer for match %d", input.MatchID)
	}

	stream := input.NewDecompressedStream()
	defer stream.Close()
	br := bufio.NewReaderSize(stream, 64*1024)

	headerPeek, err := br.Peek(8)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to decompress replay stream: %w", err)
	}

	var result BoilerplateResult
	if bytes.HasPrefix(headerPeek, []byte("PBDEMS2")) {
		res, err := p.parseSource2WithManta(ctx, ctrl, br, stream, input)
		if err != nil {
			return nil, err
		}
		result = *res
	} else {
		res, err := p.parseSyntheticStream(ctx, ctrl, br, stream, input)
		if err != nil {
			return nil, err
		}
		result = *res
	}

	result.CompressedBytes = len(input.CompressedBz2)
	result.ParseTimeMs = time.Since(start).Milliseconds()
	rawJSON, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	if ctrl != nil {
		ctrl.ReportProgress(100.0)
	}

	return &ParseOutput{
		ParserName:    p.Name(),
		ParserVersion: p.Version(),
		ParserSHA256:  DefaultParserSHA256,
		Payload:       rawJSON,
	}, nil
}

func resolveCombatLogName(p *manta.Parser, idx uint32) string {
	if s, ok := p.LookupStringByIndex("CombatLogNames", int32(idx)); ok && s != "" {
		return s
	}
	return fmt.Sprintf("name_idx_%d", idx)
}

func (p *BoilerplateMantaParser) parseSource2WithManta(
	ctx context.Context,
	ctrl ExecutionController,
	r io.Reader,
	tracked *TrackedStream,
	input ParseInput,
) (*BoilerplateResult, error) {
	mantaParser, err := manta.NewStreamParser(r)
	if err != nil {
		return nil, fmt.Errorf("manta.NewStreamParser failed: %w", err)
	}

	players := make([]PlayerSummary, 10)
	for i := 0; i < 10; i++ {
		team := "radiant"
		slot := i
		if i >= 5 {
			team = "dire"
			slot = 128 + (i - 5)
		}
		players[i] = PlayerSummary{
			PlayerSlot: slot,
			Team:       team,
		}
	}

	out := &BoilerplateResult{
		MatchID:         input.MatchID,
		Cluster:         input.Cluster,
		Region:          input.Region,
		DurationSeconds: input.DurationHintSec,
		RadiantWin:      true,
		Winner:          "radiant",
		Players:         players,
		Wards:           make([]WardPlacement, 0),
		Combat:          make(map[string]CombatSummary),
		CombatLog:       make([]CombatLogEvent, 0, 256),
		Objectives:      make([]ObjectiveEvent, 0),
		Chat:            make([]ChatEntry, 0),
		Pings:           make([]MapPing, 0),
		GameEvents:      make([]GameChatEvent, 0),
		ParsedInRAM:     true,
	}

	seenWardEntities := make(map[int32]bool)
	var packetCounter uint32
	var lastSampleMinute = -1

	samplePlayerEntities := func(curSec int) {
		prEnts := mantaParser.FilterEntity(func(e *manta.Entity) bool {
			return e != nil && e.GetClassName() == "CDOTA_PlayerResource"
		})
		dataRad := mantaParser.FilterEntity(func(e *manta.Entity) bool {
			return e != nil && e.GetClassName() == "CDOTA_DataRadiant"
		})
		dataDire := mantaParser.FilterEntity(func(e *manta.Entity) bool {
			return e != nil && e.GetClassName() == "CDOTA_DataDire"
		})

		var pr *manta.Entity
		if len(prEnts) > 0 {
			pr = prEnts[0]
		}
		var dr, dd *manta.Entity
		if len(dataRad) > 0 {
			dr = dataRad[0]
		}
		if len(dataDire) > 0 {
			dd = dataDire[0]
		}

		for i := 0; i < 10; i++ {
			pl := &out.Players[i]
			sample := PlayerSample{TimeSec: curSec}

			if pr != nil {
				pfxTeam := fmt.Sprintf("m_vecPlayerTeamData.%04d.", i)
				pfxData := fmt.Sprintf("m_vecPlayerData.%04d.", i)
				if name, ok := entityString(pr, pfxData+"m_iszPlayerName"); ok && name != "" {
					pl.PlayerName = name
				}
				if sid, ok := entityUint64(pr, pfxData+"m_iPlayerSteamID"); ok && sid > 0 {
					pl.AccountID = sid
				}
				if hid, ok := entityInt32(pr, pfxTeam+"m_nSelectedHeroID"); ok && hid > 0 {
					pl.HeroID = hid
				}
				if k, ok := entityInt32(pr, pfxTeam+"m_iKills"); ok {
					pl.Kills = k
				}
				if d, ok := entityInt32(pr, pfxTeam+"m_iDeaths"); ok {
					pl.Deaths = d
				}
				if a, ok := entityInt32(pr, pfxTeam+"m_iAssists"); ok {
					pl.Assists = a
				}
				if lvl, ok := entityInt32(pr, pfxTeam+"m_iLevel"); ok {
					pl.Level = lvl
					sample.Level = lvl
				}

				if heroHandle, ok := pr.GetUint64(pfxTeam + "m_hSelectedHero"); ok && heroHandle > 0 {
					if heroEnt := mantaParser.FindEntityByHandle(heroHandle); heroEnt != nil {
						if pl.HeroName == "" {
							pl.HeroName = heroEnt.GetClassName()
						}
						if cx, ok := heroEnt.GetUint64("CBodyComponent.m_cellX"); ok {
							sample.X = cx
						}
						if cy, ok := heroEnt.GetUint64("CBodyComponent.m_cellY"); ok {
							sample.Y = cy
						}
						if hp, ok := entityInt32(heroEnt, "m_iHealth"); ok {
							sample.Health = hp
						}
						pl.Items, pl.NeutralItem = extractHeroInventory(mantaParser, heroEnt)
					}
				}
			}

			var teamEnt *manta.Entity
			localIdx := i
			if i < 5 {
				teamEnt = dr
			} else {
				teamEnt = dd
				localIdx = i - 5
			}
			if teamEnt != nil {
				pfx := fmt.Sprintf("m_vecDataTeam.%04d.", localIdx)
				if nw, ok := entityInt32(teamEnt, pfx+"m_iNetWorth"); ok {
					pl.NetWorth = nw
					sample.NetWorth = nw
				}
				if g, ok := entityInt32(teamEnt, pfx+"m_iTotalEarnedGold"); ok {
					pl.Gold = g
					sample.Gold = g
				}
				if lh, ok := entityInt32(teamEnt, pfx+"m_iLastHits"); ok {
					pl.LastHits = lh
					sample.LastHits = lh
				}
				if dn, ok := entityInt32(teamEnt, pfx+"m_iDenies"); ok {
					pl.Denies = dn
					sample.Denies = dn
				}
				if xp, ok := entityInt32(teamEnt, pfx+"m_iTotalEarnedXP"); ok {
					sample.XP = xp
				}
				if obs, ok := entityInt32(teamEnt, pfx+"m_iObserverWardsPlaced"); ok {
					pl.ObsPlaced = obs
				}
				if sen, ok := entityInt32(teamEnt, pfx+"m_iSentryWardsPlaced"); ok {
					pl.SenPlaced = sen
				}
			}

			if curSec > 0 && curSec%60 == 0 {
				pl.MinuteSample = append(pl.MinuteSample, sample)
			}
		}
	}

	mantaParser.Callbacks.OnCDemoFileHeader(func(m *dota.CDemoFileHeader) error {
		out.MapName = m.GetMapName()
		out.BuildNum = m.GetBuildNum()
		return nil
	})

	mantaParser.Callbacks.OnCDemoPacket(func(m *dota.CDemoPacket) error {
		packetCounter++
		curMin := int(mantaParser.Tick / (30 * 60))
		if curMin > lastSampleMinute && curMin >= 0 {
			lastSampleMinute = curMin
			samplePlayerEntities(curMin * 60)
		}
		if packetCounter%40 == 0 && ctrl != nil {
			if err := ctrl.CheckGate(ctx); err != nil {
				return err
			}
			ctrl.ReportProgress(tracked.ProgressPercent())
		}
		return nil
	})

	mantaParser.Callbacks.OnCDemoFileInfo(func(m *dota.CDemoFileInfo) error {
		if m.GetPlaybackTime() > 0 {
			out.DurationSeconds = int(m.GetPlaybackTime())
		}
		if m.GetPlaybackTicks() > 0 {
			out.TotalTicks = uint32(m.GetPlaybackTicks())
		}
		if dotaInfo := m.GetGameInfo().GetDota(); dotaInfo != nil {
			out.GameMode = dotaInfo.GetGameMode()
			out.LeagueID = dotaInfo.GetLeagueid()
			out.RadiantTeamID = dotaInfo.GetRadiantTeamId()
			out.DireTeamID = dotaInfo.GetDireTeamId()
			out.RadiantTeamTag = dotaInfo.GetRadiantTeamTag()
			out.DireTeamTag = dotaInfo.GetDireTeamTag()
			out.EndTime = dotaInfo.GetEndTime()

			winner := dotaInfo.GetGameWinner()
			out.RadiantWin = winner == 2
			if out.RadiantWin {
				out.Winner = "radiant"
			} else {
				out.Winner = "dire"
			}

			for _, pb := range dotaInfo.GetPicksBans() {
				out.PicksBans = append(out.PicksBans, DraftPickBan{
					IsPick: pb.GetIsPick(),
					Team:   pb.GetTeam(),
					HeroID: pb.GetHeroId(),
				})
			}
			for i, pi := range dotaInfo.GetPlayerInfo() {
				if i >= len(out.Players) {
					break
				}
				if out.Players[i].HeroName == "" && pi.GetHeroName() != "" {
					out.Players[i].HeroName = pi.GetHeroName()
				}
				if out.Players[i].PlayerName == "" && pi.GetPlayerName() != "" {
					out.Players[i].PlayerName = pi.GetPlayerName()
				}
				if out.Players[i].AccountID == 0 && pi.GetSteamid() > 0 {
					out.Players[i].AccountID = pi.GetSteamid()
				}
			}
		}
		return nil
	})

	mantaParser.Callbacks.OnCMsgDOTACombatLogEntry(func(m *dota.CMsgDOTACombatLogEntry) error {
		logType := m.GetType()
		typeName := logType.String()
		attacker := resolveCombatLogName(mantaParser, m.GetAttackerName())
		target := resolveCombatLogName(mantaParser, m.GetTargetName())
		inflictor := ""
		if m.GetInflictorName() > 0 {
			inflictor = resolveCombatLogName(mantaParser, m.GetInflictorName())
		}
		ts := m.GetTimestamp()

		out.CombatLog = append(out.CombatLog, CombatLogEvent{
			TimeSec:        ts,
			Type:           typeName,
			Attacker:       attacker,
			Target:         target,
			Inflictor:      inflictor,
			Value:          m.GetValue(),
			StunDuration:   m.GetStunDuration(),
			AttackerHero:   m.GetIsAttackerHero(),
			TargetHero:     m.GetIsTargetHero(),
			AttackerIllus:  m.GetIsAttackerIllusion(),
			TargetIllusion: m.GetIsTargetIllusion(),
		})

		switch logType {
		case dota.DOTA_COMBATLOG_TYPES_DOTA_COMBATLOG_DAMAGE:
			cs := out.Combat[attacker]
			cs.HeroName = attacker
			cs.DamageDealt += m.GetValue()
			if m.GetIsTargetHero() && !m.GetIsTargetIllusion() {
				cs.HeroDamage += m.GetValue()
			}
			out.Combat[attacker] = cs

		case dota.DOTA_COMBATLOG_TYPES_DOTA_COMBATLOG_HEAL:
			cs := out.Combat[attacker]
			cs.HeroName = attacker
			cs.Healing += m.GetValue()
			out.Combat[attacker] = cs

		case dota.DOTA_COMBATLOG_TYPES_DOTA_COMBATLOG_DEATH:
			if m.GetIsTargetHero() && !m.GetIsTargetIllusion() {
				attCS := out.Combat[attacker]
				attCS.HeroName = attacker
				attCS.Kills++
				out.Combat[attacker] = attCS

				tgtCS := out.Combat[target]
				tgtCS.HeroName = target
				tgtCS.Deaths++
				out.Combat[target] = tgtCS
			}
			switch {
			case strings.Contains(target, "roshan"):
				out.Objectives = append(out.Objectives, ObjectiveEvent{TimeSec: ts, Type: "roshan", Unit: target, Killer: attacker, Team: m.GetAttackerTeam()})
			case strings.Contains(target, "tower"):
				out.Objectives = append(out.Objectives, ObjectiveEvent{TimeSec: ts, Type: "tower", Unit: target, Killer: attacker, Team: m.GetTargetTeam()})
			case strings.Contains(target, "rax") || strings.Contains(target, "barracks"):
				out.Objectives = append(out.Objectives, ObjectiveEvent{TimeSec: ts, Type: "barracks", Unit: target, Killer: attacker, Team: m.GetTargetTeam()})
			case strings.Contains(target, "observer_wards") || strings.Contains(target, "sentry_wards"):
				wType := "observer"
				if strings.Contains(target, "sentry") {
					wType = "sentry"
				}
				out.Wards = append(out.Wards, WardPlacement{
					TimeSec: int(ts),
					X:       int(m.GetLocationX()),
					Y:       int(m.GetLocationY()),
					Type:    wType,
					Team:    int(m.GetTargetTeam()),
					Event:   "destroyed",
				})
			}

		case dota.DOTA_COMBATLOG_TYPES_DOTA_COMBATLOG_BUYBACK:
			out.Objectives = append(out.Objectives, ObjectiveEvent{TimeSec: ts, Type: "buyback", Unit: target, Player: int32(m.GetValue())})
		}
		return nil
	})

	mantaParser.Callbacks.OnCDOTAUserMsg_ChatMessage(func(m *dota.CDOTAUserMsg_ChatMessage) error {
		out.Chat = append(out.Chat, ChatEntry{
			TimeSec:  float32(mantaParser.Tick) / 30.0,
			PlayerID: m.GetSourcePlayerId(),
			Channel:  m.GetChannelType(),
			Text:     m.GetMessageText(),
		})
		return nil
	})

	mantaParser.Callbacks.OnCDOTAUserMsg_ChatEvent(func(m *dota.CDOTAUserMsg_ChatEvent) error {
		out.GameEvents = append(out.GameEvents, GameChatEvent{
			TimeSec:   float32(mantaParser.Tick) / 30.0,
			Type:      m.GetType().String(),
			Value:     m.GetValue(),
			PlayerID1: m.GetPlayerid_1(),
			PlayerID2: m.GetPlayerid_2(),
		})
		if m.GetType() == dota.DOTA_CHAT_MESSAGE_CHAT_MESSAGE_AEGIS || m.GetType() == dota.DOTA_CHAT_MESSAGE_CHAT_MESSAGE_AEGIS_STOLEN {
			out.Objectives = append(out.Objectives, ObjectiveEvent{
				TimeSec: float32(mantaParser.Tick) / 30.0,
				Type:    "aegis",
				Player:  m.GetPlayerid_1(),
			})
		}
		return nil
	})

	mantaParser.Callbacks.OnCDOTAUserMsg_LocationPing(func(m *dota.CDOTAUserMsg_LocationPing) error {
		if lp := m.GetLocationPing(); lp != nil {
			out.Pings = append(out.Pings, MapPing{
				TimeSec:  float32(mantaParser.Tick) / 30.0,
				PlayerID: m.GetPlayerId(),
				X:        lp.GetX(),
				Y:        lp.GetY(),
				Type:     lp.GetType(),
				Direct:   lp.GetDirectPing(),
			})
		}
		return nil
	})

	mantaParser.OnEntity(func(e *manta.Entity, op manta.EntityOp) error {
		if e == nil || !op.Flag(manta.EntityOpCreated) {
			return nil
		}
		className := e.GetClassName()
		if className != "CDOTA_NPC_Observer_Ward" && className != "CDOTA_NPC_Observer_Ward_TrueSight" {
			return nil
		}
		idx := e.GetIndex()
		if seenWardEntities[idx] {
			return nil
		}
		seenWardEntities[idx] = true

		wardType := "observer"
		if className == "CDOTA_NPC_Observer_Ward_TrueSight" {
			wardType = "sentry"
		}
		cellX, _ := e.GetUint64("CBodyComponent.m_cellX")
		cellY, _ := e.GetUint64("CBodyComponent.m_cellY")
		team, _ := e.GetInt32("m_iTeamNum")

		out.Wards = append(out.Wards, WardPlacement{
			TimeSec: int(mantaParser.Tick / 30),
			X:       int(cellX),
			Y:       int(cellY),
			Type:    wardType,
			Team:    int(team),
			Event:   "placed",
		})
		return nil
	})

	if err := mantaParser.Start(); err != nil && err != io.EOF {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		out.PartialEntityNote = err.Error()
		drainBuf := make([]byte, 64*1024)
		for {
			if ctrl != nil {
				if gErr := ctrl.CheckGate(ctx); gErr != nil {
					return nil, gErr
				}
			}
			_, dErr := r.Read(drainBuf)
			if ctrl != nil {
				ctrl.ReportProgress(tracked.ProgressPercent())
			}
			if dErr != nil {
				break
			}
		}
	}

	// Capture final end-of-match player stats & roll up HeroDamage/Healing from combat log
	samplePlayerEntities(out.DurationSeconds)
	durMin := float64(out.DurationSeconds) / 60.0
	for i := range out.Players {
		pl := &out.Players[i]
		if durMin > 0 && pl.Gold > 0 {
			pl.GPM = int(float64(pl.Gold) / durMin)
		}
		if cs, ok := out.Combat[pl.HeroName]; ok {
			pl.HeroDamage = cs.HeroDamage
			pl.HeroHealing = cs.Healing
		}
	}

	if out.TotalTicks == 0 {
		out.TotalTicks = mantaParser.Tick
	}
	if out.TotalTicks == 0 && out.DurationSeconds > 0 {
		out.TotalTicks = uint32(out.DurationSeconds * 30)
	}
	return out, nil
}

func extractHeroInventory(p *manta.Parser, hero *manta.Entity) ([]string, string) {
	items := make([]string, 0, 9)
	neutral := ""
	for slot := 0; slot <= 16; slot++ {
		h, ok := hero.GetUint64(fmt.Sprintf("m_hItems.%04d", slot))
		if !ok || h == 0 || h == 0xFFFFFF {
			continue
		}
		itemEnt := p.FindEntityByHandle(h)
		if itemEnt == nil {
			continue
		}
		name := itemEnt.GetClassName()
		if slot == 16 {
			neutral = name
		} else {
			items = append(items, name)
		}
	}
	return items, neutral
}

func entityInt32(e *manta.Entity, key string) (int32, bool) {
	if v, ok := e.GetInt32(key); ok {
		return v, true
	}
	if v, ok := e.GetUint32(key); ok {
		return int32(v), true
	}
	return 0, false
}

func entityUint64(e *manta.Entity, key string) (uint64, bool) {
	if v, ok := e.GetUint64(key); ok {
		return v, true
	}
	return 0, false
}

func entityString(e *manta.Entity, key string) (string, bool) {
	return e.GetString(key)
}

func (p *BoilerplateMantaParser) parseSyntheticStream(
	ctx context.Context,
	ctrl ExecutionController,
	r io.Reader,
	tracked *TrackedStream,
	input ParseInput,
) (*BoilerplateResult, error) {
	buf := make([]byte, 4096)
	for {
		if ctrl != nil {
			if err := ctrl.CheckGate(ctx); err != nil {
				return nil, err
			}
		}
		_, err := r.Read(buf)
		if ctrl != nil {
			ctrl.ReportProgress(tracked.ProgressPercent())
		}
		time.Sleep(8 * time.Millisecond)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	dur := input.DurationHintSec
	if dur <= 0 {
		dur = 2145
	}
	radiantWin := input.MatchID%2 == 0
	winner := "dire"
	if radiantWin {
		winner = "radiant"
	}

	return &BoilerplateResult{
		MatchID:         input.MatchID,
		Cluster:         input.Cluster,
		Region:          input.Region,
		DurationSeconds: dur,
		RadiantWin:      radiantWin,
		Winner:          winner,
		MapName:         "dota",
		BuildNum:        10240,
		GameMode:        22,
		TotalTicks:      uint32(dur * 30),
		PicksBans: []DraftPickBan{
			{IsPick: true, Team: 2, HeroID: 74},
			{IsPick: true, Team: 3, HeroID: 8},
		},
		Players: []PlayerSummary{
			{PlayerSlot: 0, Team: "radiant", HeroID: 74, HeroName: "npc_dota_hero_invoker", Level: 25, Kills: 12, Deaths: 3, Assists: 14, LastHits: 310, Denies: 18, NetWorth: 24500, GPM: 685, HeroDamage: 34210, Items: []string{"item_travel_boots", "item_black_king_bar", "item_aghanims_scepter"}},
			{PlayerSlot: 128, Team: "dire", HeroID: 8, HeroName: "npc_dota_hero_juggernaut", Level: 24, Kills: 9, Deaths: 6, Assists: 8, LastHits: 340, Denies: 22, NetWorth: 23100, GPM: 645, HeroDamage: 28900, Items: []string{"item_phase_boots", "item_bfury", "item_manta"}},
		},
		Wards: []WardPlacement{
			{TimeSec: 120, X: 128, Y: 84, Type: "observer", Team: 2, Event: "placed"},
			{TimeSec: 340, X: 142, Y: 96, Type: "sentry", Team: 3, Event: "placed"},
			{TimeSec: 615, X: 110, Y: 132, Type: "observer", Team: 2, Event: "placed"},
		},
		Combat: map[string]CombatSummary{
			"npc_dota_hero_invoker":    {HeroName: "npc_dota_hero_invoker", DamageDealt: 34210, HeroDamage: 34210, Kills: 12, Deaths: 3},
			"npc_dota_hero_juggernaut": {HeroName: "npc_dota_hero_juggernaut", DamageDealt: 28900, HeroDamage: 28900, Kills: 9, Deaths: 6},
		},
		CombatLog: []CombatLogEvent{
			{TimeSec: 410.5, Type: "DOTA_COMBATLOG_DEATH", Attacker: "npc_dota_hero_invoker", Target: "npc_dota_hero_juggernaut", Value: 1, AttackerHero: true, TargetHero: true},
		},
		Objectives: []ObjectiveEvent{
			{TimeSec: 920.0, Type: "tower", Unit: "npc_dota_badguys_tower1_mid", Killer: "npc_dota_hero_invoker", Team: 3},
		},
		Chat:       []ChatEntry{},
		Pings:      []MapPing{},
		GameEvents: []GameChatEvent{},
		ParsedInRAM: true,
	}, nil
}
