package models

import (
	"fmt"
	"strings"
)

// ClusterToRegion maps Valve Dota 2 replay cluster IDs to macro-regions.
var clusterToRegionMap = map[uint32]MacroRegion{
	// US West (Seattle)
	111: RegionNA, 112: RegionNA, 113: RegionNA, 114: RegionNA, 117: RegionNA, 118: RegionNA,
	// US East (Sterling, VA)
	121: RegionNA, 122: RegionNA, 123: RegionNA, 124: RegionNA,
	// Europe West (Luxembourg / Frankfurt)
	131: RegionEU, 132: RegionEU, 133: RegionEU, 134: RegionEU, 135: RegionEU, 136: RegionEU, 137: RegionEU, 138: RegionEU,
	// South Korea / Asia
	141: RegionSEA, 142: RegionSEA, 143: RegionSEA, 144: RegionSEA, 145: RegionSEA,
	// Southeast Asia (Singapore / Manila)
	151: RegionSEA, 152: RegionSEA, 153: RegionSEA, 154: RegionSEA, 155: RegionSEA, 156: RegionSEA,
	// Dubai / Middle East -> routed to EU/SEA closest
	161: RegionEU, 163: RegionEU,
	// Australia (Sydney)
	171: RegionOCE, 172: RegionOCE,
	// Europe North / Stockholm (Russia)
	181: RegionEU, 182: RegionEU, 183: RegionEU, 184: RegionEU, 185: RegionEU, 186: RegionEU, 187: RegionEU, 188: RegionEU,
	// Europe East (Vienna)
	191: RegionEU, 192: RegionEU, 193: RegionEU,
	// South America (Brazil / Peru / Chile / Argentina)
	200: RegionSA, 201: RegionSA, 202: RegionSA, 203: RegionSA, 204: RegionSA,
	241: RegionSA, 242: RegionSA, 251: RegionSA,
	// South Africa -> EU fallback
	211: RegionEU, 212: RegionEU, 213: RegionEU,
	// China (Perfect World Telecom / Unicom)
	221: RegionCN, 222: RegionCN, 223: RegionCN, 224: RegionCN, 225: RegionCN, 227: RegionCN,
	231: RegionCN, 232: RegionCN, 236: RegionCN,
}

// ResolveClusterRegion maps a Valve cluster ID to a MacroRegion.
func ResolveClusterRegion(cluster uint32) MacroRegion {
	if reg, ok := clusterToRegionMap[cluster]; ok {
		return reg
	}
	switch {
	case cluster >= 110 && cluster <= 129:
		return RegionNA
	case cluster >= 130 && cluster <= 139:
		return RegionEU
	case cluster >= 140 && cluster <= 159:
		return RegionSEA
	case cluster >= 170 && cluster <= 179:
		return RegionOCE
	case cluster >= 180 && cluster <= 199:
		return RegionEU
	case cluster >= 200 && cluster <= 209, cluster >= 240 && cluster <= 259:
		return RegionSA
	case cluster >= 220 && cluster <= 239:
		return RegionCN
	default:
		return RegionEU
	}
}

// BuildReplayURL constructs the canonical Valve CDN URL for a match replay.
func BuildReplayURL(cluster uint32, matchID uint64, replaySalt uint64) string {
	return fmt.Sprintf("http://replay%d.valve.net/570/%d_%d.dem.bz2", cluster, matchID, replaySalt)
}

// CountryToRegion maps ISO-3166-1 alpha-2 country codes (e.g. from CF-IPCountry) to MacroRegions.
func CountryToRegion(countryCode string) MacroRegion {
	cc := strings.ToUpper(strings.TrimSpace(countryCode))
	switch cc {
	case "US", "CA", "MX":
		return RegionNA
	case "BR", "AR", "CL", "PE", "CO", "UY", "PY", "BO", "EC", "VE":
		return RegionSA
	case "SG", "MY", "PH", "ID", "TH", "VN", "KR", "JP", "TW", "HK", "IN":
		return RegionSEA
	case "AU", "NZ":
		return RegionOCE
	case "CN":
		return RegionCN
	default:
		if cc != "" {
			return RegionEU
		}
		return RegionEU
	}
}

// FilterOptionMeta describes a selectable option in the Admin UI Filter Selector.
type FilterOptionMeta struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// LobbyTypeCatalog provides human-readable lobby types for the Admin UI.
var LobbyTypeCatalog = []FilterOptionMeta{
	{ID: 7, Name: "Ranked Matchmaking", Description: "Standard Ranked All Pick / Captains Mode"},
	{ID: 0, Name: "Normal / Unranked", Description: "Public unranked matchmaking"},
	{ID: 9, Name: "Battle Cup", Description: "Weekend tournament matches"},
	{ID: 2, Name: "Tournament", Description: "Official / ticketed tournament lobbies"},
	{ID: 1, Name: "Practice Lobby", Description: "Custom practice / scrim lobbies"},
	{ID: 12, Name: "Aghanim's / Event", Description: "Special seasonal event lobbies"},
}

// GameModeCatalog provides human-readable game modes for the Admin UI.
var GameModeCatalog = []FilterOptionMeta{
	{ID: 22, Name: "All Draft (Ranked All Pick)", Description: "Standard alternating ban/pick mode"},
	{ID: 2, Name: "Captains Mode", Description: "Competitive captains draft"},
	{ID: 1, Name: "All Pick", Description: "Classic unranked all pick"},
	{ID: 3, Name: "Random Draft", Description: "Draft from a subset of 33 heroes"},
	{ID: 4, Name: "Single Draft", Description: "Pick from 4 random heroes (Low Priority / SD)"},
	{ID: 18, Name: "Ability Draft", Description: "Draft individual hero abilities"},
	{ID: 23, Name: "Turbo", Description: "Fast-paced gold/XP multiplier mode"},
}
