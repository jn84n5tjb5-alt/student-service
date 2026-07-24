# === 第一阶段：构建阶段 ===
# 使用 Go 1.23（因为 1.25 可能不存在，用 1.23 兼容）
FROM golang:1.25-alpine AS builder

# 设置 Go 代理（国内加速）
ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /app

# 复制依赖文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 编译
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o main .

# === 第二阶段：运行阶段 ===
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata
ENV TZ=Asia/Shanghai

WORKDIR /root/

COPY --from=builder /app/main .
COPY config.yaml .

EXPOSE 8080

CMD ["./main"]