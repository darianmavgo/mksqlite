package zip

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"time"
)

// FastZipEntry holds the metadata we need for listing
type FastZipEntry struct {
	Name             string
	Comment          string
	Modified         time.Time
	UncompressedSize uint64
	CompressedSize   uint64
	CRC32            uint32
	IsDir            bool
}

// ParseCentralDirectoryFast reads the Central Directory from a ReaderAt without downloading the whole file.
func ParseCentralDirectoryFast(r io.ReaderAt, fileSize int64) ([]FastZipEntry, int64, error) {
	// 1. Find and parse EOCD
	cdOffset, cdSize, numEntries, err := locateCentralDirectory(r, fileSize)
	if err != nil {
		return nil, 0, err
	}

	// 2. Read entire central directory
	cdData := make([]byte, cdSize)
	if _, err := r.ReadAt(cdData, cdOffset); err != nil {
		return nil, 0, fmt.Errorf("failed to read central directory: %w", err)
	}

	// 3. Parse entries
	entries, err := parseCDEntries(cdData, numEntries)
	if err != nil {
		return nil, 0, err
	}

	return entries, int64(cdSize), nil
}

func locateCentralDirectory(r io.ReaderAt, fileSize int64) (cdOffset int64, cdSize uint64, numEntries uint64, err error) {
	const classicEOCDSig = 0x06054b50
	const zip64EOCLocatorSig = 0x07064b50
	const zip64EOCDSig = 0x06064b50
	const maxEOCDSearch int64 = 65557 + 20

	searchSize := maxEOCDSearch
	if fileSize < searchSize {
		searchSize = fileSize
	}

	buf := make([]byte, searchSize)
	if _, err := r.ReadAt(buf, fileSize-searchSize); err != nil {
		return 0, 0, 0, err
	}

	// Find classic EOCD
	classicEOCDOffset := int64(-1)
	for i := len(buf) - 22; i >= 0; i-- {
		if binary.LittleEndian.Uint32(buf[i:i+4]) == classicEOCDSig {
			classicEOCDOffset = fileSize - int64(len(buf)-i)
			break
		}
	}
	if classicEOCDOffset == -1 {
		return 0, 0, 0, fmt.Errorf("classic EOCD not found")
	}

	// Read classic EOCD
	classicEOCD := make([]byte, 22)
	if _, err := r.ReadAt(classicEOCD, classicEOCDOffset); err != nil {
		return 0, 0, 0, err
	}

	cdOffset32 := binary.LittleEndian.Uint32(classicEOCD[16:20])
	cdSize32 := binary.LittleEndian.Uint32(classicEOCD[12:16])
	numEntries16 := binary.LittleEndian.Uint16(classicEOCD[10:12])

	// Check for ZIP64
	if cdOffset32 == 0xFFFFFFFF || cdSize32 == 0xFFFFFFFF || numEntries16 == 0xFFFF {
		locatorOffset := classicEOCDOffset - 20
		if locatorOffset < 0 {
			return 0, 0, 0, fmt.Errorf("no space for ZIP64 locator")
		}

		locator := make([]byte, 20)
		if _, err := r.ReadAt(locator, locatorOffset); err != nil {
			return 0, 0, 0, err
		}
		if binary.LittleEndian.Uint32(locator[0:4]) != zip64EOCLocatorSig {
			return 0, 0, 0, fmt.Errorf("ZIP64 locator signature missing")
		}

		zip64EOCDOffset := int64(binary.LittleEndian.Uint64(locator[8:16]))
		if zip64EOCDOffset < 0 || zip64EOCDOffset >= fileSize {
			return 0, 0, 0, fmt.Errorf("invalid ZIP64 EOCD offset")
		}

		zip64EOCDHeader := make([]byte, 56)
		if _, err := r.ReadAt(zip64EOCDHeader, zip64EOCDOffset); err != nil {
			return 0, 0, 0, err
		}
		if binary.LittleEndian.Uint32(zip64EOCDHeader[0:4]) != zip64EOCDSig {
			return 0, 0, 0, fmt.Errorf("ZIP64 EOCD signature missing")
		}

		numEntries = binary.LittleEndian.Uint64(zip64EOCDHeader[32:40])
		cdSize = binary.LittleEndian.Uint64(zip64EOCDHeader[40:48])
		cdOffset = int64(binary.LittleEndian.Uint64(zip64EOCDHeader[48:56]))
	} else {
		cdOffset = int64(cdOffset32)
		cdSize = uint64(cdSize32)
		numEntries = uint64(numEntries16)
	}

	if cdOffset < 0 || cdOffset+int64(cdSize) > fileSize {
		return 0, 0, 0, fmt.Errorf("invalid central directory location")
	}

	return cdOffset, cdSize, numEntries, nil
}

func parseCDEntries(cdData []byte, numEntries uint64) ([]FastZipEntry, error) {
	const cdHeaderSig = 0x02014b50
	var entries []FastZipEntry
	pos := 0

	for i := uint64(0); i < numEntries; i++ {
		if pos+46 > len(cdData) {
			return nil, fmt.Errorf("truncated central directory")
		}

		header := cdData[pos : pos+46]
		if binary.LittleEndian.Uint32(header[0:4]) != cdHeaderSig {
			return nil, fmt.Errorf("invalid CD header at entry %d", i)
		}

		// method := binary.LittleEndian.Uint16(header[10:12])
		modTime := binary.LittleEndian.Uint16(header[12:14])
		modDate := binary.LittleEndian.Uint16(header[14:16])
		crc32 := binary.LittleEndian.Uint32(header[16:20])
		compressedSize32 := binary.LittleEndian.Uint32(header[20:24])
		uncompressedSize32 := binary.LittleEndian.Uint32(header[24:28])
		fileNameLen := binary.LittleEndian.Uint16(header[28:30])
		extraLen := binary.LittleEndian.Uint16(header[30:32])
		commentLen := binary.LittleEndian.Uint16(header[32:34])
		localHeaderOffset32 := binary.LittleEndian.Uint32(header[42:46])

		actualUncompressedSize := uint64(uncompressedSize32)
		actualCompressedSize := uint64(compressedSize32)
		// actualLocalHeaderOffset := int64(localHeaderOffset32)

		// Parse ZIP64 extra fields if needed
		if compressedSize32 == 0xFFFFFFFF || uncompressedSize32 == 0xFFFFFFFF || localHeaderOffset32 == 0xFFFFFFFF {
			extraStart := pos + 46 + int(fileNameLen)
			extraEnd := extraStart + int(extraLen)
			if extraEnd > len(cdData) {
				return nil, fmt.Errorf("extra field out of bounds")
			}
			extra := cdData[extraStart:extraEnd]
			epos := 0
			for epos+4 <= len(extra) {
				tag := binary.LittleEndian.Uint16(extra[epos : epos+2])
				size := binary.LittleEndian.Uint16(extra[epos+2 : epos+4])
				epos += 4
				if epos+int(size) > len(extra) {
					break
				}

				if tag == 0x0001 { // ZIP64
					data := extra[epos : epos+int(size)]
					dpos := 0
					if uncompressedSize32 == 0xFFFFFFFF && dpos+8 <= len(data) {
						actualUncompressedSize = binary.LittleEndian.Uint64(data[dpos : dpos+8])
						dpos += 8
					}
					if compressedSize32 == 0xFFFFFFFF && dpos+8 <= len(data) {
						actualCompressedSize = binary.LittleEndian.Uint64(data[dpos : dpos+8])
						dpos += 8
					}
					// Offset handled if needed, but we don't use it for metadata listing
				}
				epos += int(size)
			}
		}

		nameStart := pos + 46
		nameEnd := nameStart + int(fileNameLen)
		if nameEnd > len(cdData) {
			return nil, fmt.Errorf("filename out of bounds")
		}
		name := string(cdData[nameStart:nameEnd])

		// Comment
		commentStart := pos + 46 + int(fileNameLen) + int(extraLen)
		commentEnd := commentStart + int(commentLen)
		comment := ""
		if commentEnd <= len(cdData) {
			comment = string(cdData[commentStart:commentEnd])
		}

		// Parse MS-DOS time
		modified := msdosTime(modDate, modTime)

		isDir := false
		if len(name) > 0 && name[len(name)-1] == '/' {
			isDir = true
		} else {
			// fallback check - sometimes isDir is in external attributes but trailing slash is reliable enough for now
			// or check externalAttrs := binary.LittleEndian.Uint32(header[38:42])
		}

		entries = append(entries, FastZipEntry{
			Name:             name,
			Comment:          comment,
			Modified:         modified,
			UncompressedSize: actualUncompressedSize,
			CompressedSize:   actualCompressedSize,
			CRC32:            crc32,
			IsDir:            isDir,
		})

		pos += 46 + int(fileNameLen) + int(extraLen) + int(commentLen)
	}

	log.Printf("FastZip: parsed %d entries", len(entries))
	return entries, nil
}

func msdosTime(dd, dt uint16) time.Time {
	return time.Date(
		int(dd>>9)+1980,
		time.Month((dd>>5)&0x0F),
		int(dd&0x1F),
		int(dt>>11),
		int((dt>>5)&0x3F),
		int((dt&0x1F)*2),
		0,
		time.UTC,
	)
}
