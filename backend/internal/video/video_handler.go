package video

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/apierror"
	"feedsystem_video_go/internal/middleware/jwt"
	"feedsystem_video_go/internal/storage"

	"github.com/gin-gonic/gin"
)

type VideoHandler struct {
	service        *VideoService
	accountService *account.AccountService
	storage        storage.Storage
}

func NewVideoHandler(service *VideoService, accountService *account.AccountService, storage storage.Storage) *VideoHandler {
	return &VideoHandler{service: service, accountService: accountService, storage: storage}
}

func (vh *VideoHandler) PublishVideo(c *gin.Context) {
	var req PublishVideoRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}

	authorId, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	username, err := jwt.GetUsername(c)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	video := &Video{
		AuthorID:    authorId,
		Username:    username,
		Title:       req.Title,
		Description: req.Description,
		PlayURL:     req.PlayURL,
		CoverURL:    req.CoverURL,
		CreateTime:  time.Now(),
	}
	if err := vh.service.Publish(c.Request.Context(), video); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, video)
}

func (vh *VideoHandler) UploadVideo(c *gin.Context) {
	authorId, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	f, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file"})
		return
	}

	const maxSize = 200 << 20
	if f.Size <= 0 || f.Size > maxSize {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid file size"})
		return
	}

	ext := strings.ToLower(getExt(f.Filename))
	if ext != ".mp4" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "only .mp4 is allowed"})
		return
	}

	key, err := buildMediaKey("videos", authorId, ext)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate filename"})
		return
	}

	src, err := f.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read file"})
		return
	}
	defer src.Close()

	if err := vh.storage.Put(c.Request.Context(), key, src, f.Size, "video/mp4"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	url, err := vh.storage.Resolve(c.Request.Context(), key)
	if err != nil {
		url = key
	}
	c.JSON(http.StatusOK, gin.H{
		"key":      key,
		"url":      url,
		"play_url": url,
	})
}

func (vh *VideoHandler) UploadCover(c *gin.Context) {
	authorId, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	f, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file"})
		return
	}

	const maxSize = 10 << 20
	if f.Size <= 0 || f.Size > maxSize {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid file size"})
		return
	}

	ext := strings.ToLower(getExt(f.Filename))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".webp":
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "only .jpg/.jpeg/.png/.webp is allowed"})
		return
	}

	key, err := buildMediaKey("covers", authorId, ext)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate filename"})
		return
	}

	src, err := f.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read file"})
		return
	}
	defer src.Close()

	if err := vh.storage.Put(c.Request.Context(), key, src, f.Size, contentTypeOf(ext)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	url, err := vh.storage.Resolve(c.Request.Context(), key)
	if err != nil {
		url = key
	}
	c.JSON(http.StatusOK, gin.H{
		"key":       key,
		"url":       url,
		"cover_url": url,
	})
}

func randHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("rand.Read: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func getExt(name string) string {
	idx := strings.LastIndexByte(name, '.')
	if idx < 0 {
		return ""
	}
	return name[idx:]
}

func contentTypeOf(ext string) string {
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".webp":
		return "image/webp"
	case ".mp4":
		return "video/mp4"
	default:
		return "application/octet-stream"
	}
}

// buildMediaKey 生成稳定的对象 key：{dir}/{authorId}/{yyyyMMdd}/{rand}{ext}。
// 这个 key 会落库（video.play_url / cover_url 等），读取时再解析成可访问 URL。
func buildMediaKey(dir string, authorId uint, ext string) (string, error) {
	filename, err := randHex(16)
	if err != nil {
		return "", err
	}
	date := time.Now().Format("20060102")
	return fmt.Sprintf("%s/%d/%s/%s%s", dir, authorId, date, filename, ext), nil
}

// resolveVideoURLs 把视频实体里的 key（play_url/cover_url）就地解析成可访问 URL。
// 必须在「响应边界」做，不能落库——预签名 URL 会过期。
func (vh *VideoHandler) resolveVideoURLs(c *gin.Context, v *Video) {
	if v == nil {
		return
	}
	if u, err := vh.storage.Resolve(c.Request.Context(), v.PlayURL); err == nil {
		v.PlayURL = u
	}
	if u, err := vh.storage.Resolve(c.Request.Context(), v.CoverURL); err == nil {
		v.CoverURL = u
	}
}

func (vh *VideoHandler) DeleteVideo(c *gin.Context) {
	var req DeleteVideoRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	authorId, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if err := vh.service.Delete(c.Request.Context(), req.ID, authorId); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "video deleted"})
}

func (vh *VideoHandler) ListByAuthorID(c *gin.Context) {
	var req ListByAuthorIDRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	videos, err := vh.service.ListByAuthorID(c.Request.Context(), req.AuthorID)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if videos == nil {
		videos = []Video{}
	}
	for i := range videos {
		vh.resolveVideoURLs(c, &videos[i])
	}
	c.JSON(200, videos)
}

func (vh *VideoHandler) GetDetail(c *gin.Context) {
	var req GetDetailRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	video, err := vh.service.GetDetail(c.Request.Context(), req.ID)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	vh.resolveVideoURLs(c, video)
	c.JSON(200, video)
}

func (vh *VideoHandler) UpdateLikesCount(c *gin.Context) {
	var req UpdateLikesCountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if err := vh.service.UpdateLikesCount(c.Request.Context(), req.ID, req.LikesCount); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"message": "likes count updated"})
}
